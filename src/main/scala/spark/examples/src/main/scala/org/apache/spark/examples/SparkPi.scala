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
import org.apache.spark.scheduler.local._

import akka.dispatch.verification._

import org.slf4j.LoggerFactory
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger

import scala.collection.mutable.SynchronizedQueue


class SparkViolation extends ViolationFingerprint {
  def matches(other: ViolationFingerprint) : Boolean = false
  def affectedNodes(): Seq[String] = Seq.empty
}

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
      case StatusUpdate(id, state, _) =>
        ("StatusUpdate", id, state).toString
      case m =>
        ""
    }

    if (str != "") {
      return Some(BasicFingerprint(str))
    }
    return None
  }

  override def get_external_thread(msg: Any) : Option[Long] = {
    msg match {
      case StatusUpdate(id, _, _) => return Some(id)
      case _ => return None
    }
  }
}

/** Computes an approximation to pi */
object SparkPi {
  def main(args: Array[String]) {
    def run() {
      println("Starting SparkPi")
      val conf = new SparkConf().setAppName("Spark Pi")
      val spark = new SparkContext(conf)
      val slices = if (args.length > 0) args(0).toInt else 2
      val n = 100000 * slices
      val count = spark.parallelize(1 to n, slices).map { i =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x*x + y*y < 1) 1 else 0
      }.reduce(_ + _)
      println("Pi is roughly " + 4.0 * count / n)
      Instrumenter().scheduler.asInstanceOf[ExternalEventInjector[_]].beginUnignorableEvents
      spark.stop()
      Instrumenter().executionEnded
      Instrumenter().scheduler.asInstanceOf[ExternalEventInjector[_]].endUnignorableEvents
    }

    // ---- STS ----
    /*
    def urlses(cl: ClassLoader): Array[java.net.URL] = cl match {
      case null => Array()
      case u: java.net.URLClassLoader => u.getURLs() ++ urlses(cl.getParent)
      case _ => urlses(cl.getParent)
    }

    val  urls = urlses(getClass.getClassLoader)
    println("CLASSPATH")
    println(urls.filterNot(_.toString.contains("ivy")).mkString("\n"))
    */

    // Override configs: set level to trace
    val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
    root.setLevel(Level.TRACE)

    Instrumenter().waitForExecutionStart
    val sched = new RandomScheduler(1,
                        new FingerprintFactory,
                        false,
                        0,
                        true)
    Instrumenter().scheduler = sched
    def invariant(s: Seq[akka.dispatch.verification.ExternalEvent],
                  c: scala.collection.mutable.HashMap[String,Option[akka.dispatch.verification.CheckpointReply]])
                : Option[akka.dispatch.verification.ViolationFingerprint] = {
      return None
    }
    sched.setInvariant(invariant)

    val prefix = Array[ExternalEvent](
      WaitCondition(() => false))

    sched.nonBlockingExplore(prefix,
      (ret: Option[(EventTrace,ViolationFingerprint)]) => println("STS DONE!"))

    // TODO(cs): having this line after nonBlockingExplore may not be correct
    sched.beginUnignorableEvents

    // ---- /STS ----

    run()

    Instrumenter().reset_cancellables
    Instrumenter().reset_per_system_state
    Instrumenter()._actorSystem = null

    val fingerprintFactory = new FingerprintFactory
    fingerprintFactory.registerFingerprinter(new SparkMessageFingerprinter)
    // We know that there are no external messages. So, easy way to get around
    // "deadLetters" issue: mark all messages as not from "deadLetters"
    // TODO(cs): find a more principled way to do this.
    val mappedEvents = new SynchronizedQueue[Event]
    mappedEvents ++= sched.event_orchestrator.events.events.map {
      case UniqueMsgSend(MsgSend("deadLetters", rcv, msg), id) =>
         UniqueMsgSend(MsgSend("external", rcv, msg), id)
      case e => e
    }
    val mappedEventTrace = new EventTrace(mappedEvents, sched.event_orchestrator.events.original_externals)
    val sts = new STSScheduler(mappedEventTrace, false, fingerprintFactory, false, true, false, false)
    Instrumenter().scheduler = sts
    sts.setInvariant(invariant)
    val fakeViolation = new SparkViolation
    val fakeStats = new MinimizationStats("FAKE", "STSSched")

    sts.beginUnignorableEvents
    sts.test(prefix, fakeViolation, fakeStats, Some(run))
  }
}
