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

package org.apache.spark.sql.parquet

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Row}
import org.apache.spark.sql.catalyst.types.{DataType, StringType, IntegerType}
import org.apache.spark.sql.{parquet, SchemaRDD}
import org.apache.spark.util.Utils

// Implicits
import org.apache.spark.sql.hive.test.TestHive._

class HiveParquetSuite extends FunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  val dirname = Utils.createTempDir()

  var testRDD: SchemaRDD = null

  override def beforeAll() {
    // write test data
    ParquetTestData.writeFile
    testRDD = parquetFile(ParquetTestData.testDir.toString)
    testRDD.registerAsTable("testsource")
  }

  override def afterAll() {
    Utils.deleteRecursively(ParquetTestData.testDir)
    Utils.deleteRecursively(dirname)
    reset() // drop all tables that were registered as part of the tests
  }

  // in case tests are failing we delete before and after each test
  override def beforeEach() {
    Utils.deleteRecursively(dirname)
  }

  override def afterEach() {
    Utils.deleteRecursively(dirname)
  }

  test("SELECT on Parquet table") {
    val rdd = hql("SELECT * FROM testsource").collect()
    assert(rdd != null)
    assert(rdd.forall(_.size == 6))
  }

  test("Simple column projection + filter on Parquet table") {
    val rdd = hql("SELECT myboolean, mylong FROM testsource WHERE myboolean=true").collect()
    assert(rdd.size === 5, "Filter returned incorrect number of rows")
    assert(rdd.forall(_.getBoolean(0)), "Filter returned incorrect Boolean field value")
  }

  test("Converting Hive to Parquet Table via saveAsParquetFile") {
    hql("SELECT * FROM src").saveAsParquetFile(dirname.getAbsolutePath)
    parquetFile(dirname.getAbsolutePath).registerAsTable("ptable")
    val rddOne = hql("SELECT * FROM src").collect().sortBy(_.getInt(0))
    val rddTwo = hql("SELECT * from ptable").collect().sortBy(_.getInt(0))
    compareRDDs(rddOne, rddTwo, "src (Hive)", Seq("key:Int", "value:String"))
  }

  test("INSERT OVERWRITE TABLE Parquet table") {
    hql("SELECT * FROM testsource").saveAsParquetFile(dirname.getAbsolutePath)
    parquetFile(dirname.getAbsolutePath).registerAsTable("ptable")
    // let's do three overwrites for good measure
    hql("INSERT OVERWRITE TABLE ptable SELECT * FROM testsource").collect()
    hql("INSERT OVERWRITE TABLE ptable SELECT * FROM testsource").collect()
    hql("INSERT OVERWRITE TABLE ptable SELECT * FROM testsource").collect()
    val rddCopy = hql("SELECT * FROM ptable").collect()
    val rddOrig = hql("SELECT * FROM testsource").collect()
    assert(rddCopy.size === rddOrig.size, "INSERT OVERWRITE changed size of table??")
    compareRDDs(rddOrig, rddCopy, "testsource", ParquetTestData.testSchemaFieldNames)
  }

  private def compareRDDs(rddOne: Array[Row], rddTwo: Array[Row], tableName: String, fieldNames: Seq[String]) {
    var counter = 0
    (rddOne, rddTwo).zipped.foreach {
      (a,b) => (a,b).zipped.toArray.zipWithIndex.foreach {
        case ((value_1, value_2), index) =>
          assert(value_1 === value_2, s"table $tableName row $counter field ${fieldNames(index)} don't match")
      }
    counter = counter + 1
    }
  }
}
