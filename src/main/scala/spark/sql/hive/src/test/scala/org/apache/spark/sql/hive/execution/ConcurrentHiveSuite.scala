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

package org.apache.spark.sql.hive.execution

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.test.TestHiveContext
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class ConcurrentHiveSuite extends FunSuite with BeforeAndAfterAll {
  ignore("multiple instances not supported") {
    test("Multiple Hive Instances") {
      (1 to 10).map { i =>
        val ts =
          new TestHiveContext(new SparkContext("local", s"TestSQLContext$i", new SparkConf()))
        ts.executeSql("SHOW TABLES").toRdd.collect()
        ts.executeSql("SELECT * FROM src").toRdd.collect()
        ts.executeSql("SHOW TABLES").toRdd.collect()
      }
    }
  }
}
