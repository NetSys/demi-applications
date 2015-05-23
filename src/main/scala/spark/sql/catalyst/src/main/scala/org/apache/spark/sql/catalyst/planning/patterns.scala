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

package org.apache.spark.sql.catalyst.planning

import scala.annotation.tailrec

import org.apache.spark.sql.Logging

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._

/**
 * A pattern that matches any number of filter operations on top of another relational operator.
 * Adjacent filter operators are collected and their conditions are broken up and returned as a
 * sequence of conjunctive predicates.
 *
 * @return A tuple containing a sequence of conjunctive predicates that should be used to filter the
 *         output and a relational operator.
 */
object FilteredOperation extends PredicateHelper {
  type ReturnType = (Seq[Expression], LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = Some(collectFilters(Nil, plan))

  @tailrec
  private def collectFilters(filters: Seq[Expression], plan: LogicalPlan): ReturnType = plan match {
    case Filter(condition, child) =>
      collectFilters(filters ++ splitConjunctivePredicates(condition), child)
    case other => (filters, other)
  }
}

/**
 * A pattern that matches any number of project or filter operations on top of another relational
 * operator.  All filter operators are collected and their conditions are broken up and returned
 * together with the top project operator.
 * [[org.apache.spark.sql.catalyst.expressions.Alias Aliases]] are in-lined/substituted if
 * necessary.
 */
object PhysicalOperation extends PredicateHelper {
  type ReturnType = (Seq[NamedExpression], Seq[Expression], LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    val (fields, filters, child, _) = collectProjectsAndFilters(plan)
    Some((fields.getOrElse(child.output), filters, child))
  }

  /**
   * Collects projects and filters, in-lining/substituting aliases if necessary.  Here are two
   * examples for alias in-lining/substitution.  Before:
   * {{{
   *   SELECT c1 FROM (SELECT key AS c1 FROM t1) t2 WHERE c1 > 10
   *   SELECT c1 AS c2 FROM (SELECT key AS c1 FROM t1) t2 WHERE c1 > 10
   * }}}
   * After:
   * {{{
   *   SELECT key AS c1 FROM t1 WHERE key > 10
   *   SELECT key AS c2 FROM t1 WHERE key > 10
   * }}}
   */
  def collectProjectsAndFilters(plan: LogicalPlan):
      (Option[Seq[NamedExpression]], Seq[Expression], LogicalPlan, Map[Attribute, Expression]) =
    plan match {
      case Project(fields, child) =>
        val (_, filters, other, aliases) = collectProjectsAndFilters(child)
        val substitutedFields = fields.map(substitute(aliases)).asInstanceOf[Seq[NamedExpression]]
        (Some(substitutedFields), filters, other, collectAliases(substitutedFields))

      case Filter(condition, child) =>
        val (fields, filters, other, aliases) = collectProjectsAndFilters(child)
        val substitutedCondition = substitute(aliases)(condition)
        (fields, filters ++ splitConjunctivePredicates(substitutedCondition), other, aliases)

      case other =>
        (None, Nil, other, Map.empty)
    }

  def collectAliases(fields: Seq[Expression]) = fields.collect {
    case a @ Alias(child, _) => a.toAttribute.asInstanceOf[Attribute] -> child
  }.toMap

  def substitute(aliases: Map[Attribute, Expression])(expr: Expression) = expr.transform {
    case a @ Alias(ref: AttributeReference, name) =>
      aliases.get(ref).map(Alias(_, name)(a.exprId, a.qualifiers)).getOrElse(a)

    case a: AttributeReference =>
      aliases.get(a).map(Alias(_, a.name)(a.exprId, a.qualifiers)).getOrElse(a)
  }
}

/**
 * A pattern that finds joins with equality conditions that can be evaluated using equi-join.
 */
object ExtractEquiJoinKeys extends Logging with PredicateHelper {
  /** (joinType, rightKeys, leftKeys, condition, leftChild, rightChild) */
  type ReturnType =
    (JoinType, Seq[Expression], Seq[Expression], Option[Expression], LogicalPlan, LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case join @ Join(left, right, joinType, condition) =>
      logger.debug(s"Considering join on: $condition")
      // Find equi-join predicates that can be evaluated before the join, and thus can be used
      // as join keys.
      val (joinPredicates, otherPredicates) = 
        condition.map(splitConjunctivePredicates).getOrElse(Nil).partition {
          case EqualTo(l, r) if (canEvaluate(l, left) && canEvaluate(r, right)) ||
            (canEvaluate(l, right) && canEvaluate(r, left)) => true
          case _ => false
        }

      val joinKeys = joinPredicates.map {
        case EqualTo(l, r) if canEvaluate(l, left) && canEvaluate(r, right) => (l, r)
        case EqualTo(l, r) if canEvaluate(l, right) && canEvaluate(r, left) => (r, l)
      }
      val leftKeys = joinKeys.map(_._1)
      val rightKeys = joinKeys.map(_._2)

      if (joinKeys.nonEmpty) {
        logger.debug(s"leftKeys:${leftKeys} | rightKeys:${rightKeys}")
        Some((joinType, leftKeys, rightKeys, otherPredicates.reduceOption(And), left, right))
      } else {
        None
      }
    case _ => None
  }
}

/**
 * A pattern that collects all adjacent unions and returns their children as a Seq.
 */
object Unions {
  def unapply(plan: LogicalPlan): Option[Seq[LogicalPlan]] = plan match {
    case u: Union => Some(collectUnionChildren(u))
    case _ => None
  }

  private def collectUnionChildren(plan: LogicalPlan): Seq[LogicalPlan] = plan match {
    case Union(l, r) => collectUnionChildren(l) ++ collectUnionChildren(r)
    case other => other :: Nil
  }
}
