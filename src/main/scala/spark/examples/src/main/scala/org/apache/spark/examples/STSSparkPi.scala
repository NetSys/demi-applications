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
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages
import org.apache.spark.deploy._

import akka.dispatch.verification._

import org.slf4j.LoggerFactory
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger

import scala.collection.mutable.SynchronizedQueue
import scala.collection.mutable.ListBuffer

import org.apache.spark.deploy.DeployMessages

import org.apache.spark.executor.ExecutorURLClassLoader

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean

import java.net.URL
import java.net.URLClassLoader

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

object WorkerCreator {
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

object MyVariables {
  // Global variables here to support serialization of CodeBLocks (e.g., we
  // don't want each closure to have its own spark variable)
  var spark : SparkContext = null
  var future : SimpleFutureAction[Int] = null
  var prematureStopSempahore = new Semaphore(0)
  var stsReturn : Option[(EventTrace,ViolationFingerprint)] = None
  var doneSubmittingJob = new AtomicBoolean(false)
}

/** Computes an approximation to pi */
object STSSparkPi {
  def main(args: Array[String]) {
    EventTypes.setExternalMessageFilter(STSSparkPi.externalMessageFilter)
    // TODO(cs): Hack: remove me after the deadline
    Instrumenter.setSynchronizeOnScheduler(false)

    def resetSharedVariables() {
      MyVariables.spark = null
      MyVariables.future = null
      MyVariables.prematureStopSempahore = new Semaphore(0)
      MyVariables.doneSubmittingJob = new AtomicBoolean(false)
    }

    def run(jobId: Int) {
      val firstRun = (jobId == 0)

      if (MyVariables.spark == null) {
        println("Starting SparkPi")
        val conf = new SparkConf().setAppName("Spark Pi").setSparkHome("/Users/cs/Research/UCB/code/sts2-applications/src/main/scala/spark")
        MyVariables.spark = new SparkContext(conf)
      }
      val slices = if (args.length > 0) args(0).toInt else 2
      val n = slices

      println("Submitting job")

      val partitions = if (firstRun)
        //          NODE_LOCAL                NODE_LOCAL         ANY       ANY
        Seq((0,Seq("localhost", "0")),(1,Seq("localhost", "0")),(2,Seq()),(3,Seq()))
        //        ANY
        else Seq((1,Seq()))

      val mapRdds = MyVariables.spark.makeRDD(partitions).map(MyJob.mapFunction)

      try {
        if (firstRun) {
          MyVariables.future = mapRdds.reduceNonBlocking(jobId, MyJob.reduceFunction)
        } else {
          mapRdds.reduceNonBlocking(jobId, MyJob.reduceFunction)
        }
      } catch {
        case e: SparkException => println("WARN Submitting didnt work")
      }

      if (firstRun) {
        // We've finished the bootstrapping phase.
        Instrumenter().scheduler.asInstanceOf[ExternalEventInjector[_]].endUnignorableEvents
        MyVariables.doneSubmittingJob.set(true)

        // Block until either the job is complete or a violation was found.
        MyVariables.prematureStopSempahore.acquire
        println("Pi is roughly FOO")
      }
    }

    def terminationCallback(ret: Option[(EventTrace,ViolationFingerprint)]) {
      // Either a violation was found, or the WaitCondition returned true i.e.
      // the job completed.
      MyVariables.stsReturn = ret
      println("TERMINATING!")
      MyVariables.prematureStopSempahore.release()
    }

    def cleanup() {
      if (MyVariables.spark != null) {
        TaskSetManager.decisions.clear

        if (Instrumenter().scheduler.asInstanceOf[ExternalEventInjector[_]].unignorableEvents.get()) {
          Instrumenter().scheduler.asInstanceOf[ExternalEventInjector[_]].endUnignorableEvents
        }
        // TODO(cs): also ensure that atomic blocks are marked off properly?

        // don't pay attention to shutdown messages.
        Instrumenter().setPassthrough
        Instrumenter().interruptAllScheduleBlocks
        try {
          println("invoking spark.stop()")
          MyVariables.spark.stop()
        } catch {
          case e: Exception => println("WARN Exception during spark.stop: " + e)
        }

        // N.B. Requires us to comment out SparkEnv's actorSystem.shutdown()
        // line
        Instrumenter().shutdown_system(false)

        // So that Spark can start its own actorSystem again
        Instrumenter()._actorSystem = null
        Instrumenter().reset_per_system_state
        resetSharedVariables
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
    // Quite noisy loggers
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
      invariant_check=Some(LocalityInversion.invariant))

    // UNCOMMENT FOR SHORT RUN
    //val sched = new RandomScheduler(schedulerConfig,
    //  invariant_check_interval=3, randomizationStrategy=new SrcDstFIFO)
    //sched.setMaxMessages(1000)
    // UNCOMMENT FOR LONG RUN
    val sched = new RandomScheduler(schedulerConfig,
      invariant_check_interval=1000, randomizationStrategy=new SrcDstFIFO)
    sched.setMaxMessages(1000000)

    Instrumenter().scheduler = sched

    val prefix = Array[ExternalEvent](
      WaitCondition(() => MyVariables.doneSubmittingJob.get())) ++
      (1 to 3).map { case i => CodeBlock(() =>
        WorkerCreator.createWorker("Worker"+i)) } ++
      (1 to 3).map { case i => CodeBlock(() => run(i)) } ++
      (4 to 15).map { case i => CodeBlock(() =>
        WorkerCreator.createWorker("Worker"+i)) } ++
      (4 to 15).map { case i => CodeBlock(() => run(i)) } ++
      Array[ExternalEvent](
      WaitCondition(() => MyVariables.future != null && MyVariables.future.isCompleted))

    def preTest() {
      Instrumenter().scheduler.asInstanceOf[ExternalEventInjector[_]].beginUnignorableEvents
    }
    def postTest() {
      MyVariables.prematureStopSempahore.release()
    }

    val fuzz = true
    if (fuzz) {
      sched.nonBlockingExplore(prefix, terminationCallback)

      // TODO(cs): having this line after nonBlockingExplore may not be correct
      sched.beginUnignorableEvents

      runAndCleanup()

      // XXX actorSystem.awaitTermination blocks forever. And simply
      // proceeding causes exceptions upon trying to create new actors (even
      // after nulling out Instrumenter()._actorSystem...?). So, we do the
      // worst hack: we sleep for a bit...
      Thread.sleep(2)

      MyVariables.stsReturn match {
        case Some((initTrace, violation)) =>
          println("Violation was found! Trying replay")
          val depGraph = sched.depTracker.getGraph
          val initialTrace = sched.depTracker.getInitialTrace

          val sts = new STSScheduler(schedulerConfig, initTrace, false)
          sts.setPreTestCallback(preTest)
          sts.setPostTestCallback(postTest)

          val dag = new UnmodifiedEventDag(initTrace.original_externals flatMap {
            case WaitQuiescence() => None
            case WaitCondition(_) => None
            case e => Some(e)
          })
          // Conjoin the worker starts and failures
          // atomicPairs foreach {
          //   case ((e1, e2)) =>
          //     dag.conjoinAtoms(e1, e2)
          // }

          val (mcs, stats1, verified_mcs, _) =
          RunnerUtils.stsSchedDDMin(false, schedulerConfig,
            initTrace, violation,
            initializationRoutine=Some(runAndCleanup),
            _sched=Some(sts), dag=Some(dag))

          assert(!verified_mcs.isEmpty)

          // Just to see how much minimization there is...
          verified_mcs match {
            case Some(mcsTrace) =>
              val (internalStats, intMinTrace) = RunnerUtils.minimizeInternals(
                schedulerConfig, mcs, mcsTrace, Seq.empty, violation,
                initializationRoutine=Some(runAndCleanup), preTest=Some(preTest),
                postTest=Some(postTest))

              RunnerUtils.printMinimizationStats(fingerprintFactory,
                initTrace, None, Seq(("DDMin", mcsTrace), ("IntMin", intMinTrace)))
            case None =>
          }

          // dump to disk
          val serializer = new ExperimentSerializer(
           fingerprintFactory,
           new BasicMessageSerializer)

          val dir = serializer.record_experiment("spark-fuzz",
             initTrace, violation,
             depGraph=Some(depGraph), initialTrace=Some(initialTrace),
             filteredTrace=None)

          val mcs_dir = serializer.serializeMCS(dir, mcs, stats1,
             verified_mcs, violation, false)
          println("MCS DIR: " + mcs_dir)
        case None =>
          println("Job finished successfully...")
      }
    }

    if (!fuzz) {
      val dir =
      //"/Users/cs/Research/UCB/code/sts2-applications/experiments/spark-fuzz_2015_09_13_23_15_54"
      "/Users/cs/Research/UCB/code/sts2-applications/experiments/spark-fuzz_2015_09_14_16_08_29"
      val mcs_dir =
      //"/Users/cs/Research/UCB/code/sts2-applications/experiments/spark-fuzz_2015_09_13_23_15_54_DDMin_STSSchedNoPeek"
      "/Users/cs/Research/UCB/code/sts2-applications/experiments/spark-fuzz_2015_09_14_16_08_29_DDMin_STSSchedNoPeek"

      val msgSerializer = new BasicMessageSerializer
      val msgDeserializer = new BasicMessageDeserializer(loader=Thread.currentThread.getContextClassLoader)

      def shouldRerunDDMin(externals: Seq[ExternalEvent]) =
        false // XXX

      RunnerUtils.runTheGamut(dir, mcs_dir, schedulerConfig, msgSerializer,
        msgDeserializer, shouldRerunDDMin=shouldRerunDDMin,
        populateActors=false,
        loader=Thread.currentThread.getContextClassLoader,
        initializationRoutine=Some(runAndCleanup),
        preTest=Some(preTest), postTest=Some(postTest))
    }
  }

  def externalMessageFilter(msg: Any) = {
    msg match {
      case DeployMessages.RequestSubmitDriver(_) => true
      case _ => false
    }
  }
}
