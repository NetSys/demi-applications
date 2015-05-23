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

package org.apache.spark.sql.execution

import org.scalatest.FunSuite

import org.apache.spark.sql.TestData._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution
import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.sql.test.TestSQLContext.planner._

class PlannerSuite extends FunSuite {
  test("unions are collapsed") {
    val query = testData.unionAll(testData).unionAll(testData).logicalPlan
    val planned = BasicOperators(query).head
    val logicalUnions = query collect { case u: logical.Union => u }
    val physicalUnions = planned collect { case u: execution.Union => u }

    assert(logicalUnions.size === 2)
    assert(physicalUnions.size === 1)
  }

  test("count is partially aggregated") {
    val query = testData.groupBy('value)(Count('key)).queryExecution.analyzed
    val planned = PartialAggregation(query).head
    val aggregations = planned.collect { case a: Aggregate => a }

    assert(aggregations.size === 2)
  }

  test("count distinct is not partially aggregated") {
    val query = testData.groupBy('value)(CountDistinct('key :: Nil)).queryExecution.analyzed
    val planned = PartialAggregation(query)
    assert(planned.isEmpty)
  }

  test("mixed aggregates are not partially aggregated") {
    val query =
      testData.groupBy('value)(Count('value), CountDistinct('key :: Nil)).queryExecution.analyzed
    val planned = PartialAggregation(query)
    assert(planned.isEmpty)
  }
}
