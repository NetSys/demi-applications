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

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{OrderedDistribution, UnspecifiedDistribution}
import org.apache.spark.util.MutablePair

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class Project(projectList: Seq[NamedExpression], child: SparkPlan) extends UnaryNode {
  override def output = projectList.map(_.toAttribute)

  override def execute() = child.execute().mapPartitions { iter =>
    @transient val reusableProjection = new MutableProjection(projectList)
    iter.map(reusableProjection)
  }
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class Filter(condition: Expression, child: SparkPlan) extends UnaryNode {
  override def output = child.output

  override def execute() = child.execute().mapPartitions { iter =>
    iter.filter(condition.eval(_).asInstanceOf[Boolean])
  }
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class Sample(fraction: Double, withReplacement: Boolean, seed: Long, child: SparkPlan)
  extends UnaryNode
{
  override def output = child.output

  // TODO: How to pick seed?
  override def execute() = child.execute().sample(withReplacement, fraction, seed)
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class Union(children: Seq[SparkPlan])(@transient sqlContext: SQLContext) extends SparkPlan {
  // TODO: attributes output by union should be distinct for nullability purposes
  override def output = children.head.output
  override def execute() = sqlContext.sparkContext.union(children.map(_.execute()))

  override def otherCopyArgs = sqlContext :: Nil
}

/**
 * :: DeveloperApi ::
 * Take the first limit elements. Note that the implementation is different depending on whether
 * this is a terminal operator or not. If it is terminal and is invoked using executeCollect,
 * this operator uses something similar to Spark's take method on the Spark driver. If it is not
 * terminal or is invoked using execute, we first take the limit on each partition, and then
 * repartition all the data to a single partition to compute the global limit.
 */
@DeveloperApi
case class Limit(limit: Int, child: SparkPlan)(@transient sqlContext: SQLContext)
  extends UnaryNode {
  // TODO: Implement a partition local limit, and use a strategy to generate the proper limit plan:
  // partition local limit -> exchange into one partition -> partition local limit again

  override def otherCopyArgs = sqlContext :: Nil

  override def output = child.output

  /**
   * A custom implementation modeled after the take function on RDDs but which never runs any job
   * locally.  This is to avoid shipping an entire partition of data in order to retrieve only a few
   * rows.
   */
  override def executeCollect(): Array[Row] = {
    if (limit == 0) {
      return new Array[Row](0)
    }

    val childRDD = child.execute().map(_.copy())

    val buf = new ArrayBuffer[Row]
    val totalParts = childRDD.partitions.length
    var partsScanned = 0
    while (buf.size < limit && partsScanned < totalParts) {
      // The number of partitions to try in this iteration. It is ok for this number to be
      // greater than totalParts because we actually cap it at totalParts in runJob.
      var numPartsToTry = 1
      if (partsScanned > 0) {
        // If we didn't find any rows after the first iteration, just try all partitions next.
        // Otherwise, interpolate the number of partitions we need to try, but overestimate it
        // by 50%.
        if (buf.size == 0) {
          numPartsToTry = totalParts - 1
        } else {
          numPartsToTry = (1.5 * limit * partsScanned / buf.size).toInt
        }
      }
      numPartsToTry = math.max(0, numPartsToTry)  // guard against negative num of partitions

      val left = limit - buf.size
      val p = partsScanned until math.min(partsScanned + numPartsToTry, totalParts)
      val sc = sqlContext.sparkContext
      val res =
        sc.runJob(childRDD, (it: Iterator[Row]) => it.take(left).toArray, p, allowLocal = false)

      res.foreach(buf ++= _.take(limit - buf.size))
      partsScanned += numPartsToTry
    }

    buf.toArray
  }

  override def execute() = {
    val rdd = child.execute().mapPartitions { iter =>
      val mutablePair = new MutablePair[Boolean, Row]()
      iter.take(limit).map(row => mutablePair.update(false, row))
    }
    val part = new HashPartitioner(1)
    val shuffled = new ShuffledRDD[Boolean, Row, Row, MutablePair[Boolean, Row]](rdd, part)
    shuffled.setSerializer(new SparkSqlSerializer(new SparkConf(false)))
    shuffled.mapPartitions(_.take(limit).map(_._2))
  }
}

/**
 * :: DeveloperApi ::
 * Take the first limit elements as defined by the sortOrder. This is logically equivalent to
 * having a [[Limit]] operator after a [[Sort]] operator. This could have been named TopK, but
 * Spark's top operator does the opposite in ordering so we name it TakeOrdered to avoid confusion.
 */
@DeveloperApi
case class TakeOrdered(limit: Int, sortOrder: Seq[SortOrder], child: SparkPlan)
                      (@transient sqlContext: SQLContext) extends UnaryNode {
  override def otherCopyArgs = sqlContext :: Nil

  override def output = child.output

  @transient
  lazy val ordering = new RowOrdering(sortOrder)

  override def executeCollect() = child.execute().map(_.copy()).takeOrdered(limit)(ordering)

  // TODO: Terminal split should be implemented differently from non-terminal split.
  // TODO: Pick num splits based on |limit|.
  override def execute() = sqlContext.sparkContext.makeRDD(executeCollect(), 1)
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class Sort(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan)
  extends UnaryNode {
  override def requiredChildDistribution =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  @transient
  lazy val ordering = new RowOrdering(sortOrder)

  override def execute() = attachTree(this, "sort") {
    // TODO: Optimize sorting operation?
    child.execute()
      .mapPartitions(
        iterator => iterator.map(_.copy()).toArray.sorted(ordering).iterator,
        preservesPartitioning = true)
  }

  override def output = child.output
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
object ExistingRdd {
  def convertToCatalyst(a: Any): Any = a match {
    case o: Option[_] => o.orNull
    case s: Seq[Any] => s.map(convertToCatalyst)
    case p: Product => new GenericRow(p.productIterator.map(convertToCatalyst).toArray)
    case other => other
  }

  def productToRowRdd[A <: Product](data: RDD[A]): RDD[Row] = {
    data.mapPartitions { iterator =>
      if (iterator.isEmpty) {
        Iterator.empty
      } else {
        val bufferedIterator = iterator.buffered
        val mutableRow = new GenericMutableRow(bufferedIterator.head.productArity)

        bufferedIterator.map { r =>
          var i = 0
          while (i < mutableRow.length) {
            mutableRow(i) = convertToCatalyst(r.productElement(i))
            i += 1
          }

          mutableRow
        }
      }
    }
  }

  def fromProductRdd[A <: Product : TypeTag](productRdd: RDD[A]) = {
    ExistingRdd(ScalaReflection.attributesFor[A], productToRowRdd(productRdd))
  }
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class ExistingRdd(output: Seq[Attribute], rdd: RDD[Row]) extends LeafNode {
  override def execute() = rdd
}

/**
 * :: DeveloperApi ::
 * Returns a table with the elements from left that are not in right using
 * the built-in spark subtract function.
 */
@DeveloperApi
case class Except(left: SparkPlan, right: SparkPlan) extends BinaryNode {
  override def output = left.output

  override def execute() = {
    left.execute().map(_.copy()).subtract(right.execute().map(_.copy()))
  }
}

/**
 * :: DeveloperApi ::
 * Returns the rows in left that also appear in right using the built in spark
 * intersection function.
 */
@DeveloperApi
case class Intersect(left: SparkPlan, right: SparkPlan) extends BinaryNode {
  override def output = children.head.output

  override def execute() = {
    left.execute().map(_.copy()).intersection(right.execute().map(_.copy()))
  }
}
