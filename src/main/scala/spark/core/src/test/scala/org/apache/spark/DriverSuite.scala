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

package org.apache.spark

import java.io.File

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.scalatest.FunSuite
import org.scalatest.concurrent.Timeouts
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.time.SpanSugar._
import org.apache.spark.util.Utils

class DriverSuite extends FunSuite with Timeouts {
  test("driver should exit after finishing") {
    assert(System.getenv("SPARK_HOME") != null)
    // Regression test for SPARK-530: "Spark driver process doesn't exit after finishing"
    val masters = Table(("master"), ("local"), ("local-cluster[2,1,512]"))
    forAll(masters) { (master: String) =>
      failAfter(30 seconds) {
        Utils.execute(Seq("./sbin/spark-class", "org.apache.spark.DriverWithoutCleanup", master),
          new File(System.getenv("SPARK_HOME")))
      }
    }
  }
}

/**
 * Program that creates a Spark driver but doesn't call SparkContext.stop() or
 * Sys.exit() after finishing.
 */
object DriverWithoutCleanup {
  def main(args: Array[String]) {
    Logger.getRootLogger().setLevel(Level.WARN)
    val sc = new SparkContext(args(0), "DriverWithoutCleanup")
    sc.parallelize(1 to 100, 4).count()
  }
}
