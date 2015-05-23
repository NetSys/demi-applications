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

package org.apache.spark.sql.catalyst

import java.sql.Timestamp

import scala.language.implicitConversions

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.catalyst.types._

/**
 * A collection of implicit conversions that create a DSL for constructing catalyst data structures.
 *
 * {{{
 *  scala> import org.apache.spark.sql.catalyst.dsl.expressions._
 *
 *  // Standard operators are added to expressions.
 *  scala> import org.apache.spark.sql.catalyst.expressions.Literal
 *  scala> Literal(1) + Literal(1)
 *  res0: org.apache.spark.sql.catalyst.expressions.Add = (1 + 1)
 *
 *  // There is a conversion from 'symbols to unresolved attributes.
 *  scala> 'a.attr
 *  res1: org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute = 'a
 *
 *  // These unresolved attributes can be used to create more complicated expressions.
 *  scala> 'a === 'b
 *  res2: org.apache.spark.sql.catalyst.expressions.EqualTo = ('a = 'b)
 *
 *  // SQL verbs can be used to construct logical query plans.
 *  scala> import org.apache.spark.sql.catalyst.plans.logical._
 *  scala> import org.apache.spark.sql.catalyst.dsl.plans._
 *  scala> LocalRelation('key.int, 'value.string).where('key === 1).select('value).analyze
 *  res3: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan =
 *  Project [value#3]
 *   Filter (key#2 = 1)
 *    LocalRelation [key#2,value#3], []
 * }}}
 */
package object dsl {
  trait ImplicitOperators {
    def expr: Expression

    def unary_- = UnaryMinus(expr)
    def unary_! = Not(expr)

    def + (other: Expression) = Add(expr, other)
    def - (other: Expression) = Subtract(expr, other)
    def * (other: Expression) = Multiply(expr, other)
    def / (other: Expression) = Divide(expr, other)
    def % (other: Expression) = Remainder(expr, other)

    def && (other: Expression) = And(expr, other)
    def || (other: Expression) = Or(expr, other)

    def < (other: Expression) = LessThan(expr, other)
    def <= (other: Expression) = LessThanOrEqual(expr, other)
    def > (other: Expression) = GreaterThan(expr, other)
    def >= (other: Expression) = GreaterThanOrEqual(expr, other)
    def === (other: Expression) = EqualTo(expr, other)
    def !== (other: Expression) = Not(EqualTo(expr, other))

    def like(other: Expression) = Like(expr, other)
    def rlike(other: Expression) = RLike(expr, other)
    def cast(to: DataType) = Cast(expr, to)

    def asc = SortOrder(expr, Ascending)
    def desc = SortOrder(expr, Descending)

    def as(s: Symbol) = Alias(expr, s.name)()
  }

  trait ExpressionConversions {
    implicit class DslExpression(e: Expression) extends ImplicitOperators {
      def expr = e
    }

    implicit def booleanToLiteral(b: Boolean) = Literal(b)
    implicit def byteToLiteral(b: Byte) = Literal(b)
    implicit def shortToLiteral(s: Short) = Literal(s)
    implicit def intToLiteral(i: Int) = Literal(i)
    implicit def longToLiteral(l: Long) = Literal(l)
    implicit def floatToLiteral(f: Float) = Literal(f)
    implicit def doubleToLiteral(d: Double) = Literal(d)
    implicit def stringToLiteral(s: String) = Literal(s)
    implicit def decimalToLiteral(d: BigDecimal) = Literal(d)
    implicit def timestampToLiteral(t: Timestamp) = Literal(t)
    implicit def binaryToLiteral(a: Array[Byte]) = Literal(a)

    implicit def symbolToUnresolvedAttribute(s: Symbol) = analysis.UnresolvedAttribute(s.name)

    def sum(e: Expression) = Sum(e)
    def sumDistinct(e: Expression) = SumDistinct(e)
    def count(e: Expression) = Count(e)
    def countDistinct(e: Expression*) = CountDistinct(e)
    def avg(e: Expression) = Average(e)
    def first(e: Expression) = First(e)
    def min(e: Expression) = Min(e)
    def max(e: Expression) = Max(e)
    def upper(e: Expression) = Upper(e)
    def lower(e: Expression) = Lower(e)

    implicit class DslSymbol(sym: Symbol) extends ImplicitAttribute { def s = sym.name }
    // TODO more implicit class for literal?
    implicit class DslString(val s: String) extends ImplicitOperators {
      override def expr: Expression = Literal(s)
      def attr = analysis.UnresolvedAttribute(s)
    }

    abstract class ImplicitAttribute extends ImplicitOperators {
      def s: String
      def expr = attr
      def attr = analysis.UnresolvedAttribute(s)

      /** Creates a new AttributeReference of type boolean */
      def boolean = AttributeReference(s, BooleanType, nullable = true)()

      /** Creates a new AttributeReference of type byte */
      def byte = AttributeReference(s, ByteType, nullable = true)()

      /** Creates a new AttributeReference of type short */
      def short = AttributeReference(s, ShortType, nullable = true)()

      /** Creates a new AttributeReference of type int */
      def int = AttributeReference(s, IntegerType, nullable = true)()

      /** Creates a new AttributeReference of type long */
      def long = AttributeReference(s, LongType, nullable = true)()

      /** Creates a new AttributeReference of type float */
      def float = AttributeReference(s, FloatType, nullable = true)()

      /** Creates a new AttributeReference of type double */
      def double = AttributeReference(s, DoubleType, nullable = true)()

      /** Creates a new AttributeReference of type string */
      def string = AttributeReference(s, StringType, nullable = true)()

      /** Creates a new AttributeReference of type decimal */
      def decimal = AttributeReference(s, DecimalType, nullable = true)()

      /** Creates a new AttributeReference of type timestamp */
      def timestamp = AttributeReference(s, TimestampType, nullable = true)()

      /** Creates a new AttributeReference of type binary */
      def binary = AttributeReference(s, BinaryType, nullable = true)()
    }

    implicit class DslAttribute(a: AttributeReference) {
      def notNull = a.withNullability(false)
      def nullable = a.withNullability(true)

      // Protobuf terminology
      def required = a.withNullability(false)

      def at(ordinal: Int) = BoundReference(ordinal, a)
    }
  }


  object expressions extends ExpressionConversions  // scalastyle:ignore

  abstract class LogicalPlanFunctions {
    def logicalPlan: LogicalPlan

    def select(exprs: NamedExpression*) = Project(exprs, logicalPlan)

    def where(condition: Expression) = Filter(condition, logicalPlan)

    def limit(limitExpr: Expression) = Limit(limitExpr, logicalPlan)

    def join(
        otherPlan: LogicalPlan,
        joinType: JoinType = Inner,
        condition: Option[Expression] = None) =
      Join(logicalPlan, otherPlan, joinType, condition)

    def orderBy(sortExprs: SortOrder*) = Sort(sortExprs, logicalPlan)

    def groupBy(groupingExprs: Expression*)(aggregateExprs: Expression*) = {
      val aliasedExprs = aggregateExprs.map {
        case ne: NamedExpression => ne
        case e => Alias(e, e.toString)()
      }
      Aggregate(groupingExprs, aliasedExprs, logicalPlan)
    }

    def subquery(alias: Symbol) = Subquery(alias.name, logicalPlan)

    def unionAll(otherPlan: LogicalPlan) = Union(logicalPlan, otherPlan)

    def sfilter[T1](arg1: Symbol)(udf: (T1) => Boolean) =
      Filter(ScalaUdf(udf, BooleanType, Seq(UnresolvedAttribute(arg1.name))), logicalPlan)

    def sfilter(dynamicUdf: (DynamicRow) => Boolean) =
      Filter(ScalaUdf(dynamicUdf, BooleanType, Seq(WrapDynamic(logicalPlan.output))), logicalPlan)

    def sample(
        fraction: Double,
        withReplacement: Boolean = true,
        seed: Int = (math.random * 1000).toInt) =
      Sample(fraction, withReplacement, seed, logicalPlan)

    def generate(
        generator: Generator,
        join: Boolean = false,
        outer: Boolean = false,
        alias: Option[String] = None) =
      Generate(generator, join, outer, None, logicalPlan)

    def insertInto(tableName: String, overwrite: Boolean = false) =
      InsertIntoTable(
        analysis.UnresolvedRelation(None, tableName), Map.empty, logicalPlan, overwrite)

    def analyze = analysis.SimpleAnalyzer(logicalPlan)
  }

  object plans {  // scalastyle:ignore
    implicit class DslLogicalPlan(val logicalPlan: LogicalPlan) extends LogicalPlanFunctions {
      def writeToFile(path: String) = WriteToFile(path, logicalPlan)
    }
  }
}
