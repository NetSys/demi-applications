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

package org.apache.spark.sql.catalyst.plans

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.types.{ArrayType, DataType, StructField, StructType}

abstract class QueryPlan[PlanType <: TreeNode[PlanType]] extends TreeNode[PlanType] {
  self: PlanType with Product =>

  def output: Seq[Attribute]

  /**
   * Returns the set of attributes that are output by this node.
   */
  def outputSet: Set[Attribute] = output.toSet

  /**
   * Runs [[transform]] with `rule` on all expressions present in this query operator.
   * Users should not expect a specific directionality. If a specific directionality is needed,
   * transformExpressionsDown or transformExpressionsUp should be used.
   * @param rule the rule to be applied to every expression in this operator.
   */
  def transformExpressions(rule: PartialFunction[Expression, Expression]): this.type = {
    transformExpressionsDown(rule)
  }

  /**
   * Runs [[transformDown]] with `rule` on all expressions present in this query operator.
   * @param rule the rule to be applied to every expression in this operator.
   */
  def transformExpressionsDown(rule: PartialFunction[Expression, Expression]): this.type = {
    var changed = false

    @inline def transformExpressionDown(e: Expression) = {
      val newE = e.transformDown(rule)
      if (newE.id != e.id && newE != e) {
        changed = true
        newE
      } else {
        e
      }
    }

    val newArgs = productIterator.map {
      case e: Expression => transformExpressionDown(e)
      case Some(e: Expression) => Some(transformExpressionDown(e))
      case m: Map[_,_] => m
      case seq: Traversable[_] => seq.map {
        case e: Expression => transformExpressionDown(e)
        case other => other
      }
      case other: AnyRef => other
    }.toArray

    if (changed) makeCopy(newArgs) else this
  }

  /**
   * Runs [[transformUp]] with `rule` on all expressions present in this query operator.
   * @param rule the rule to be applied to every expression in this operator.
   * @return
   */
  def transformExpressionsUp(rule: PartialFunction[Expression, Expression]): this.type = {
    var changed = false

    @inline def transformExpressionUp(e: Expression) = {
      val newE = e.transformUp(rule)
      if (newE.id != e.id && newE != e) {
        changed = true
        newE
      } else {
        e
      }
    }

    val newArgs = productIterator.map {
      case e: Expression => transformExpressionUp(e)
      case Some(e: Expression) => Some(transformExpressionUp(e))
      case m: Map[_,_] => m
      case seq: Traversable[_] => seq.map {
        case e: Expression => transformExpressionUp(e)
        case other => other
      }
      case other: AnyRef => other
    }.toArray

    if (changed) makeCopy(newArgs) else this
  }

  /** Returns the result of running [[transformExpressions]] on this node
    * and all its children. */
  def transformAllExpressions(rule: PartialFunction[Expression, Expression]): this.type = {
    transform {
      case q: QueryPlan[_] => q.transformExpressions(rule).asInstanceOf[PlanType]
    }.asInstanceOf[this.type]
  }

  /** Returns all of the expressions present in this query plan operator. */
  def expressions: Seq[Expression] = {
    productIterator.flatMap {
      case e: Expression => e :: Nil
      case Some(e: Expression) => e :: Nil
      case seq: Traversable[_] => seq.flatMap {
        case e: Expression => e :: Nil
        case other => Nil
      }
      case other => Nil
    }.toSeq
  }

  protected def generateSchemaString(schema: Seq[Attribute]): String = {
    val builder = new StringBuilder
    builder.append("root\n")
    val prefix = " |"
    schema.foreach { attribute =>
      val name = attribute.name
      val dataType = attribute.dataType
      dataType match {
        case fields: StructType =>
          builder.append(s"$prefix-- $name: $StructType\n")
          generateSchemaString(fields, s"$prefix    |", builder)
        case ArrayType(fields: StructType) =>
          builder.append(s"$prefix-- $name: $ArrayType[$StructType]\n")
          generateSchemaString(fields, s"$prefix    |", builder)
        case ArrayType(elementType: DataType) =>
          builder.append(s"$prefix-- $name: $ArrayType[$elementType]\n")
        case _ => builder.append(s"$prefix-- $name: $dataType\n")
      }
    }

    builder.toString()
  }

  protected def generateSchemaString(
      schema: StructType,
      prefix: String,
      builder: StringBuilder): StringBuilder = {
    schema.fields.foreach {
      case StructField(name, fields: StructType, _) =>
        builder.append(s"$prefix-- $name: $StructType\n")
        generateSchemaString(fields, s"$prefix    |", builder)
      case StructField(name, ArrayType(fields: StructType), _) =>
        builder.append(s"$prefix-- $name: $ArrayType[$StructType]\n")
        generateSchemaString(fields, s"$prefix    |", builder)
      case StructField(name, ArrayType(elementType: DataType), _) =>
        builder.append(s"$prefix-- $name: $ArrayType[$elementType]\n")
      case StructField(name, fieldType: DataType, _) =>
        builder.append(s"$prefix-- $name: $fieldType\n")
    }

    builder
  }

  /** Returns the output schema in the tree format. */
  def schemaString: String = generateSchemaString(output)

  /** Prints out the schema in the tree format */
  def printSchema(): Unit = println(schemaString)
}
