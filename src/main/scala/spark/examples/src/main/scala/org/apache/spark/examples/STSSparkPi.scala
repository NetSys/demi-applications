/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples

import scala.math.random

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.TaskLocality
import org.apache.spark.scheduler.local._
import org.apache.spark.deploy.worker.Worker
import org.apache.spark.deploy.master.Master
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages
import org.apache.spark.deploy._

import akka.dispatch.verification._

import org.slf4j.LoggerFactory
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger

import scala.collection.mutable.SynchronizedQueue
import scala.collection.mutable.ListBuffer

import org.apache.spark.deploy.DeployMessages

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean

import java.io._

import akka.actor.Props

class SparkMessageFingerprinter extends MessageFingerprinter {
  override def fingerprint(msg: Any) : Option[MessageFingerprint] = {
    val alreadyFingerprint = super.fingerprint(msg)
    if (alreadyFingerprint != None) {
      return alreadyFingerprint
    }

    val str = msg match {
      case BlockManagerMessages.RegisterBlockManager(_,_,_) =>
        "RegisterBlockManager"
      case BlockManagerMessages.HeartBeat(_) =>
        "HeartBeat"
      case JobSubmitted(jobId,_,_,_,_,_,_,_) =>
        ("JobSubmitted", jobId).toString
      case BlockManagerMessages.GetLocationsMultipleBlockIds(_) =>
        "GetLocationsMultipleBlockIds"
      case BeginEvent(task,_) =>
        ("BeginEvent", task).toString
      case CompletionEvent(task, reason, _, _, _, _) =>
        ("CompletionEvent", task, reason).toString
      case org.apache.spark.scheduler.local.StatusUpdate(id, state, _) =>
        ("StatusUpdate", id, state).toString
      case CoarseGrainedClusterMessages.StatusUpdate(execId, tid, state, _) =>
        ("StatusUpdate", execId, tid, state).toString
      case DeployMessages.RegisteredApplication(_, _) =>
        ("RegisteredApplication").toString
      case DeployMessages.ExecutorStateChanged(_, id, state, _, _) =>
        ("ExecutorStateChanged", id, state).toString
      case DeployMessages.RegisterWorker(id, host, port, cores, memory, webUiPort, publicAddress) =>
        ("RegisterWorker", id).toString
      case DeployMessages.RegisteredWorker(_, _) =>
        ("RegisteredWorker").toString
      case CoarseGrainedClusterMessages.LaunchTask(_) =>
        ("LaunchTask").toString
      case m =>
        ""
    }

    if (str != "") {
      return Some(BasicFingerprint(str))
    }
    return None
  }
}

object ActorCreator {
  def createWorker(name: String) {
    val conf = new SparkConf
    val securityMgr = new SecurityManager(conf)

    // Signal to STS that we should wait until after preStart has been
    // triggered...
    val regex = "Worker(\\d+)".r
    val id = name match {
      case regex(m) => m.toLong
      case _ => throw new IllegalStateException("WTF")
    }
    // TODO(cs): another option: treat preStart invocations as messages, to be
    // scheduled like other messages.
    if (Instrumenter().scheduler.isInstanceOf[ExternalEventInjector[_]]) {
      val sched = Instrumenter().scheduler.asInstanceOf[ExternalEventInjector[_]]
      println("beginAtomicBlock: Worker("+id+")")
      sched.beginExternalAtomicBlock(id)
    }

    Instrumenter()._actorSystem.actorOf(
      Props(classOf[Worker], "localhost", 12345, 12346, 1, 512,
            Array[String]("Master"), "foobarbaz", name, null, conf,
            securityMgr),
      name=name)
  }

  def createMaster() {
    val conf = new SparkConf
    val securityMgr = new SecurityManager(conf)

    // Signal to STS that we should wait until after preStart has been
    // triggered...
    if (Instrumenter().scheduler.isInstanceOf[ExternalEventInjector[_]]) {
      val sched = Instrumenter().scheduler.asInstanceOf[ExternalEventInjector[_]]
      println("beginAtomicBlock: Master")
      sched.beginExternalAtomicBlock(-1)
    }

    Instrumenter()._actorSystem.actorOf(
      Props(classOf[Master], "localhost", 12345, 12346, securityMgr, true), "Master")

    Instrumenter().blockedActors = Instrumenter().blockedActors - "Master"
  }
}

object MyJob {
  def mapFunction(i: Int): Int = {
    val x = random * 2 - 1
    val y = random * 2 - 1
    if (x*x + y*y < 1) 1 else 0
  }

  def reduceFunction(i: Int, j: Int): Int = {
    return i + j
  }
}

/** Computes an approximation to pi */
object STSSparkPi {
  def main(args: Array[String]) {
    var spark : SparkContext = null
    var future : SimpleFutureAction[Int] = null
    var prematureStopSempahore = new Semaphore(0)
    var stsReturn : Option[(EventTrace,ViolationFingerprint)] = None
    var doneSubmittingJob = new AtomicBoolean(false)

    def resetSharedVariables() {
      spark = null
      future = null
      prematureStopSempahore = new Semaphore(0)
      doneSubmittingJob = new AtomicBoolean(false)
    }

    // Return: (externals, atomic pairs of externals)
    def getExternals() : Tuple2[Seq[ExternalEvent], ListBuffer[Tuple2[ExternalEvent,ExternalEvent]]] = {
      val atomicPairs = new ListBuffer[Tuple2[ExternalEvent,ExternalEvent]]

      val prefix = new ListBuffer[ExternalEvent] ++ Array[ExternalEvent](
        WaitCondition(() => doneSubmittingJob.get()))

      // External events to fuzz:
      // -  (1 to 1).map { case i => CodeBlock(() =>
      //     ActorCreator.createWorker("Worker"+i)) }
      // -  (1 to 1).map { case i => CodeBlock(() => run(i)) }
      // -  Kills?
      // -  HardKills followed by recoveries [spaced apart?]

      prefix ++=
      (1 to 4).map { case i => CodeBlock(() => ActorCreator.createWorker("Worker"+i)) } ++
      (1 to 2).map { case i => CodeBlock(() => run(i)) } ++
      Array[ExternalEvent](WaitCondition(() =>
        Instrumenter().scheduler.asInstanceOf[RandomScheduler].messagesScheduledSoFar > 400),
      Kill("Worker1"),
      Kill("Worker3")) ++
      (5 to 8).map { case i => CodeBlock(() => ActorCreator.createWorker("Worker"+i)) } ++
      (3 to 7).map { case i => CodeBlock(() => run(i)) }

      val kill = HardKill("Master")
      val recover = CodeBlock(() =>
          ActorCreator.createMaster())
      atomicPairs += ((kill, recover))

      val knownMCS = Array[ExternalEvent](
        Send("Master",
          BasicMessageConstructor(DeployMessages.RequestSubmitDriver(
            new DriverDescription("", 1, 1, false,
              Command("", Seq.empty, Map.empty, Seq.empty, Seq.empty, Seq.empty))))),
        WaitCondition(() => Master.hasSubmittedDriver.get()),
        kill,
        recover)

      prefix ++= knownMCS
      // val finish = WaitCondition(() => future != null && future.isCompleted)
      val finish = WaitQuiescence()
      prefix += finish
      return (prefix, atomicPairs)
    }

    def run(jobId: Int) {
      val firstInvocation = (jobId == 0)

      if (spark == null) {
        println("Starting SparkPi")
        val conf = new SparkConf().
          setAppName("Spark Pi").
          setSparkHome("/Users/cs/Research/UCB/code/sts2-applications/src/main/scala/spark").
          set("spark.deploy.recoveryMode", "FILESYSTEM").
          set("spark.deploy.recoveryDirectory", "/tmp/spark")

        spark = new SparkContext(conf)
      }
      val slices = if (args.length > 0) args(0).toInt else 2
      val n = slices

      println("Submitting job")

      val partitions = if (firstInvocation)
        //          NODE_LOCAL                NODE_LOCAL         ANY       ANY
        Seq((0,Seq("localhost", "0")),(1,Seq("localhost", "0")),(2,Seq()),(3,Seq()))
        //        ANY
        else Seq((1,Seq()))

      val mapRdds = spark.makeRDD(partitions).map(MyJob.mapFunction)

      if (firstInvocation) {
        future = mapRdds.reduceNonBlocking(jobId, MyJob.reduceFunction)
      } else {
        mapRdds.reduceNonBlocking(jobId, MyJob.reduceFunction)
      }

      if (firstInvocation) {
        // We've finished the bootstrapping phase.
        Instrumenter().scheduler.asInstanceOf[ExternalEventInjector[_]].endUnignorableEvents
        doneSubmittingJob.set(true)

        // Block until either the job is complete or a violation was found.
        prematureStopSempahore.acquire
        println("Pi is roughly FOO")
      }
    }

    def terminationCallback(ret: Option[(EventTrace,ViolationFingerprint)]) {
      // Either a violation was found, or the WaitCondition returned true i.e.
      // the job completed.
      stsReturn = ret
      println("TERMINATING!")
      prematureStopSempahore.release()
    }

    def cleanup() {
      if (spark != null) {
        if (Instrumenter().scheduler.asInstanceOf[ExternalEventInjector[_]].unignorableEvents.get()) {
          Instrumenter().scheduler.asInstanceOf[ExternalEventInjector[_]].endUnignorableEvents
        }
        // TODO(cs): also ensure that atomic blocks are marked off properly?

        // don't pay attention to shutdown messages.
        Instrumenter().setPassthrough
        spark.stop()

        // N.B. Requires us to comment out SparkEnv's actorSystem.shutdown()
        // line
        Instrumenter().shutdown_system(false)

        // XXX actorSystem.awaitTermination blocks forever. And simply
        // proceeding causes exceptions upon trying to create new actors (even
        // after nulling out Instrumenter()._actorSystem...?). So, we do the
         // worst hack: we sleep for a bit...
        Thread.sleep(3)

        // So that Spark can start its own actorSystem again
        Instrumenter()._actorSystem = null
        Instrumenter().reset_per_system_state
        resetSharedVariables

        Master.hasCausedNPE.set(false)
        Master.hasSubmittedDriver.set(false)
        Worker.connected.set(0)

        // Delete master failover state
        for {
          files <- Option(new File("/tmp/spark").listFiles)
          file <- files
        } file.delete()
      }
    }

    def runAndCleanup() {
      try {
        // pass-through akka-remote initialization
        Instrumenter().setPassthrough // unset within Spark
        run(0)
      } finally {
        cleanup()
      }
    }

    // Override configs: set level to trace
    LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger].setLevel(Level.TRACE)
    // Quiet noisy loggers
    LoggerFactory.getLogger("org.eclipse.jetty").asInstanceOf[ch.qos.logback.classic.Logger].setLevel(Level.WARN)
    LoggerFactory.getLogger("org.eclipse.jetty.util.component.AbstractLifeCycle").asInstanceOf[ch.qos.logback.classic.Logger].setLevel(Level.ERROR)

    // Begin fuzzing..
    val fingerprintFactory = new FingerprintFactory
    fingerprintFactory.registerFingerprinter(new SparkMessageFingerprinter)

    val schedulerConfig = SchedulerConfig(
      messageFingerprinter=fingerprintFactory,
      shouldShutdownActorSystem=false,
      filterKnownAbsents=false,
      ignoreTimers=false, // XXX
      invariant_check=Some(CrashUponRecovery.invariant))

    val sched = new RandomScheduler(schedulerConfig,
      invariant_check_interval=300, randomizationStrategy=new SrcDstFIFO)
    sched.setMaxMessages(1000)
    Instrumenter().scheduler = sched

    val (externals, atomicPairs) = getExternals()

    sched.nonBlockingExplore(externals, terminationCallback)

    sched.beginUnignorableEvents

    runAndCleanup()

    stsReturn match {
      case Some((initTrace, violation)) =>
        println("Violation was found! Trying replay")

        val mappedInitTrace = STSSparkPi.removeDeadLetters(initTrace)

        val sts = new STSScheduler(schedulerConfig, mappedInitTrace, false)
        def preTest() {
          Instrumenter().scheduler.asInstanceOf[ExternalEventInjector[_]].beginUnignorableEvents
        }
        def postTest() {
          prematureStopSempahore.release()
        }
        sts.setPreTestCallback(preTest)
        sts.setPostTestCallback(postTest)

        val dag = new UnmodifiedEventDag(initTrace.original_externals flatMap {
          case WaitQuiescence() => None
          case WaitCondition(_) => None
          case e => Some(e)
        })
        // Conjoin the HardKill and the subsequent recover
        atomicPairs foreach {
          case ((e1, e2)) =>
            dag.conjoinAtoms(e1, e2)
        }

        val (mcs, stats, verified_mcs, _) = RunnerUtils.stsSchedDDMin(false, schedulerConfig,
          mappedInitTrace, violation,
          initializationRoutine=Some(runAndCleanup),
          _sched=Some(sts), dag=Some(dag))

        verified_mcs match {
          case Some(mcsTrace) =>
            val mappedMCSTrace = STSSparkPi.removeDeadLetters(mcsTrace)
            val (internalStats, intMinTrace) = RunnerUtils.minimizeInternals(
              schedulerConfig, mcs, mappedMCSTrace, Seq.empty, violation,
              initializationRoutine=Some(runAndCleanup), preTest=Some(preTest),
              postTest=Some(postTest))

            RunnerUtils.printMinimizationStats(mappedInitTrace, None, mappedMCSTrace,
              STSSparkPi.removeDeadLetters(intMinTrace), fingerprintFactory)
          case None =>
        }
      case None =>
        println("Job finished successfully...")
    }
  }

  def removeDeadLetters(trace: EventTrace): EventTrace = {
    // We know that there are no external messages. So, easy way to get around
    // "deadLetters" duplicate messages send issue: mark all messages as not from "deadLetters"
    // TODO(cs): find a more principled way to do this.
    val mappedEvents = new SynchronizedQueue[Event]
    mappedEvents ++= trace.events.map {
      case u @ UniqueMsgSend(MsgSend("deadLetters", rcv,
                             DeployMessages.RequestSubmitDriver(_)), id) =>
        u
      case UniqueMsgSend(MsgSend("deadLetters", rcv, msg), id) =>
         UniqueMsgSend(MsgSend("external", rcv, msg), id)
      case e => e
    }
    return new EventTrace(mappedEvents, trace.original_externals)
  }
}
