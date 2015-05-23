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

import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.analysis.UnresolvedException

case class Coalesce(children: Seq[Expression]) extends Expression {
  type EvaluatedType = Any

  /** Coalesce is nullable if all of its children are nullable, or if it has no children. */
  def nullable = !children.exists(!_.nullable)

  def references = children.flatMap(_.references).toSet
  // Coalesce is foldable if all children are foldable.
  override def foldable = !children.exists(!_.foldable)

  // Only resolved if all the children are of the same type.
  override lazy val resolved = childrenResolved && (children.map(_.dataType).distinct.size == 1)

  override def toString = s"Coalesce(${children.mkString(",")})"

  def dataType = if (resolved) {
    children.head.dataType
  } else {
    throw new UnresolvedException(this, "Coalesce cannot have children of different types.")
  }

  override def eval(input: Row): Any = {
    var i = 0
    var result: Any = null
    while(i < children.size && result == null) {
      result = children(i).eval(input)
      i += 1
    }
    result
  }
}

case class IsNull(child: Expression) extends Predicate with trees.UnaryNode[Expression] {
  def references = child.references
  override def foldable = child.foldable
  def nullable = false

  override def eval(input: Row): Any = {
    child.eval(input) == null
  }
}

case class IsNotNull(child: Expression) extends Predicate with trees.UnaryNode[Expression] {
  def references = child.references
  override def foldable = child.foldable
  def nullable = false
  override def toString = s"IS NOT NULL $child"

  override def eval(input: Row): Any = {
    child.eval(input) != null
  }
}
