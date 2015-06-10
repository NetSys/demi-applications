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

import akka.dispatch.verification._

import org.slf4j.LoggerFactory
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger

/** Computes an approximation to pi */
object SparkPi {
  def main(args: Array[String]) {
    def run() {
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
      spark.stop()
      // TODO(cs): probably need to null out ActorSystem at the end
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
    println("scheduler.nonBlockingExplore")
    sched.nonBlockingExplore(prefix,
      (ret: Option[(EventTrace,ViolationFingerprint)]) => println("STS DONE!"))
    sched.beginUnignorableEvents
    // ---- /STS ----

    run()

    println("events:")
    sched.event_orchestrator.events foreach { case e => println(e) }

    // val g = Instrumenter().scheduler.asInstanceOf[RandomScheduler].depTracker.getGraph
    // println(Util.getDot(g))
  }
}
