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

import akka.dispatch.verification._

import org.slf4j.LoggerFactory
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger

import scala.collection.mutable.SynchronizedQueue

import org.apache.spark.deploy.DeployMessages

import java.util.concurrent.Semaphore

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
      case JobSubmitted(_,_,_,_,_,_,_,_) =>
        "JobSubmitted" // TODO(cs): change me when there are concurrent jobs
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

/** Computes an approximation to pi */
object STSSparkPi {
  def main(args: Array[String]) {
    var spark : SparkContext = null
    var future : SimpleFutureAction[Int] = null
    val prematureStopSempahore = new Semaphore(0)
    var stsReturn : Option[(EventTrace,ViolationFingerprint)] = None

    def run() {
      println("Starting SparkPi")
      val conf = new SparkConf().setAppName("Spark Pi").setSparkHome("/Users/cs/Research/UCB/code/sts2-applications/src/main/scala/spark")
      spark = new SparkContext(conf)
      val slices = if (args.length > 0) args(0).toInt else 2
      val n = slices
      future = spark.makeRDD(
        //          NODE_LOCAL                NODE_LOCAL         ANY       ANY
        Seq((0,Seq("localhost", "0")),(1,Seq("localhost", "0")),(2,Seq()),(3,Seq()))
      ).map { i =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x*x + y*y < 1) 1 else 0
      }.reduceNonBlocking(_ + _)

      // We've finished the bootstrapping phase.
      Instrumenter().scheduler.asInstanceOf[ExternalEventInjector[_]].endUnignorableEvents

      // Block until either the job is complete or a violation was found.
      prematureStopSempahore.acquire
      println("Pi is roughly FOO")
    }

    // Override configs: set level to trace
    val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
    root.setLevel(Level.TRACE)

    val fingerprintFactory = new FingerprintFactory
    fingerprintFactory.registerFingerprinter(new SparkMessageFingerprinter)

    val schedulerConfig = SchedulerConfig(
      messageFingerprinter=fingerprintFactory,
      shouldShutdownActorSystem=false,
      filterKnownAbsents=false,
      ignoreTimers=false,
      invariant_check=Some(LocalityInversion.invariant))

    val sched = new RandomScheduler(schedulerConfig, invariant_check_interval=3)
    Instrumenter().scheduler = sched

    val prefix = Array[ExternalEvent](
      WaitCondition(() => future != null && future.isCompleted))

    def terminationCallback(ret: Option[(EventTrace,ViolationFingerprint)]) {
      // Either a violation was found, or the WaitCondition returned true i.e.
      // the job completed.
      stsReturn = ret
      prematureStopSempahore.release()
    }

    sched.nonBlockingExplore(prefix, terminationCallback)

    // TODO(cs): having this line after nonBlockingExplore may not be correct
    sched.beginUnignorableEvents

    def cleanup() {
      if (spark != null) {
        if (sched.unignorableEvents.get()) {
          sched.endUnignorableEvents
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
        Worker.workerId.set(0)
      }
    }

    def runAndCleanup() {
      try {
        Instrumenter().setPassthrough // unset within Spark
        run()
      } finally {
        cleanup()
      }
    }

    runAndCleanup()

    stsReturn match {
      case Some((trace, violation)) =>
        println("Violation was found! Trying replay")

        // We know that there are no external messages. So, easy way to get around
        // "deadLetters" duplicate messages send issue: mark all messages as not from "deadLetters"
        // TODO(cs): find a more principled way to do this.
        val mappedEvents = new SynchronizedQueue[Event]
        mappedEvents ++= trace.events.map {
          case UniqueMsgSend(MsgSend("deadLetters", rcv, msg), id) =>
             UniqueMsgSend(MsgSend("external", rcv, msg), id)
          case e => e
        }
        val mappedEventTrace = new EventTrace(mappedEvents, trace.original_externals)
        val sts = new STSScheduler(schedulerConfig, mappedEventTrace)
        Instrumenter().scheduler = sts
        val fakeStats = new MinimizationStats("FAKE", "STSSched")

        sts.beginUnignorableEvents
        sts.test(prefix, violation, fakeStats, Some(runAndCleanup))

        println("Replayed successfully!")

      case None =>
        println("Job finished successfully...")
    }
  }
}
