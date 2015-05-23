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

package org.apache.spark.sql.catalyst.expressions

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.types._

object Literal {
  def apply(v: Any): Literal = v match {
    case i: Int => Literal(i, IntegerType)
    case l: Long => Literal(l, LongType)
    case d: Double => Literal(d, DoubleType)
    case f: Float => Literal(f, FloatType)
    case b: Byte => Literal(b, ByteType)
    case s: Short => Literal(s, ShortType)
    case s: String => Literal(s, StringType)
    case b: Boolean => Literal(b, BooleanType)
    case d: BigDecimal => Literal(d, DecimalType)
    case t: Timestamp => Literal(t, TimestampType)
    case a: Array[Byte] => Literal(a, BinaryType)
    case null => Literal(null, NullType)
  }
}

/**
 * Extractor for retrieving Int literals.
 */
object IntegerLiteral {
  def unapply(a: Any): Option[Int] = a match {
    case Literal(a: Int, IntegerType) => Some(a)
    case _ => None
  }
}

case class Literal(value: Any, dataType: DataType) extends LeafExpression {

  override def foldable = true
  def nullable = value == null
  def references = Set.empty

  override def toString = if (value != null) value.toString else "null"

  type EvaluatedType = Any
  override def eval(input: Row):Any = value
}

// TODO: Specialize
case class MutableLiteral(var value: Any, nullable: Boolean = true) extends LeafExpression {
  type EvaluatedType = Any

  val dataType = Literal(value).dataType

  def references = Set.empty

  def update(expression: Expression, input: Row) = {
    value = expression.eval(input)
  }

  override def eval(input: Row) = value
}
