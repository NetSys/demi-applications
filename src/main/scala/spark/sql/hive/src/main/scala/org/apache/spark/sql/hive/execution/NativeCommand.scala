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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericRow, Row}
import org.apache.spark.sql.execution.{Command, LeafNode}
import org.apache.spark.sql.hive.HiveContext

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class NativeCommand(
    sql: String, output: Seq[Attribute])(
    @transient context: HiveContext)
  extends LeafNode with Command {

  override protected[sql] lazy val sideEffectResult: Seq[String] = context.runSqlHive(sql)

  override def execute(): RDD[Row] = {
    if (sideEffectResult.size == 0) {
      context.emptyResult
    } else {
      val rows = sideEffectResult.map(r => new GenericRow(Array[Any](r)))
      context.sparkContext.parallelize(rows, 1)
    }
  }

  override def otherCopyArgs = context :: Nil
}
