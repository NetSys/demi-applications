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

package org.apache.spark.mllib.optimization

import scala.util.Random
import scala.collection.JavaConversions._

import org.scalatest.FunSuite
import org.scalatest.Matchers

import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.mllib.linalg.Vectors

object GradientDescentSuite {

  def generateLogisticInputAsList(
      offset: Double,
      scale: Double,
      nPoints: Int,
      seed: Int): java.util.List[LabeledPoint] = {
    seqAsJavaList(generateGDInput(offset, scale, nPoints, seed))
  }

  // Generate input of the form Y = logistic(offset + scale * X)
  def generateGDInput(
      offset: Double,
      scale: Double,
      nPoints: Int,
      seed: Int): Seq[LabeledPoint]  = {
    val rnd = new Random(seed)
    val x1 = Array.fill[Double](nPoints)(rnd.nextGaussian())

    val unifRand = new scala.util.Random(45)
    val rLogis = (0 until nPoints).map { i =>
      val u = unifRand.nextDouble()
      math.log(u) - math.log(1.0-u)
    }

    val y: Seq[Int] = (0 until nPoints).map { i =>
      val yVal = offset + scale * x1(i) + rLogis(i)
      if (yVal > 0) 1 else 0
    }

    (0 until nPoints).map(i => LabeledPoint(y(i), Vectors.dense(x1(i))))
  }
}

class GradientDescentSuite extends FunSuite with LocalSparkContext with Matchers {

  test("Assert the loss is decreasing.") {
    val nPoints = 10000
    val A = 2.0
    val B = -1.5

    val initialB = -1.0
    val initialWeights = Array(initialB)

    val gradient = new LogisticGradient()
    val updater = new SimpleUpdater()
    val stepSize = 1.0
    val numIterations = 10
    val regParam = 0
    val miniBatchFrac = 1.0

    // Add a extra variable consisting of all 1.0's for the intercept.
    val testData = GradientDescentSuite.generateGDInput(A, B, nPoints, 42)
    val data = testData.map { case LabeledPoint(label, features) =>
      label -> Vectors.dense(1.0 +: features.toArray)
    }

    val dataRDD = sc.parallelize(data, 2).cache()
    val initialWeightsWithIntercept = Vectors.dense(1.0 +: initialWeights.toArray)

    val (_, loss) = GradientDescent.runMiniBatchSGD(
      dataRDD,
      gradient,
      updater,
      stepSize,
      numIterations,
      regParam,
      miniBatchFrac,
      initialWeightsWithIntercept)

    assert(loss.last - loss.head < 0, "loss isn't decreasing.")

    val lossDiff = loss.init.zip(loss.tail).map { case (lhs, rhs) => lhs - rhs }
    assert(lossDiff.count(_ > 0).toDouble / lossDiff.size > 0.8)
  }

  test("Test the loss and gradient of first iteration with regularization.") {

    val gradient = new LogisticGradient()
    val updater = new SquaredL2Updater()

    // Add a extra variable consisting of all 1.0's for the intercept.
    val testData = GradientDescentSuite.generateGDInput(2.0, -1.5, 10000, 42)
    val data = testData.map { case LabeledPoint(label, features) =>
      label -> Vectors.dense(1.0 +: features.toArray)
    }

    val dataRDD = sc.parallelize(data, 2).cache()

    // Prepare non-zero weights
    val initialWeightsWithIntercept = Vectors.dense(1.0, 0.5)

    val regParam0 = 0
    val (newWeights0, loss0) = GradientDescent.runMiniBatchSGD(
      dataRDD, gradient, updater, 1, 1, regParam0, 1.0, initialWeightsWithIntercept)

    val regParam1 = 1
    val (newWeights1, loss1) = GradientDescent.runMiniBatchSGD(
      dataRDD, gradient, updater, 1, 1, regParam1, 1.0, initialWeightsWithIntercept)

    def compareDouble(x: Double, y: Double, tol: Double = 1E-3): Boolean = {
      math.abs(x - y) / (math.abs(y) + 1e-15) < tol
    }

    assert(compareDouble(
      loss1(0),
      loss0(0) + (math.pow(initialWeightsWithIntercept(0), 2) +
        math.pow(initialWeightsWithIntercept(1), 2)) / 2),
      """For non-zero weights, the regVal should be \frac{1}{2}\sum_i w_i^2.""")

    assert(
      compareDouble(newWeights1(0) , newWeights0(0) - initialWeightsWithIntercept(0)) &&
      compareDouble(newWeights1(1) , newWeights0(1) - initialWeightsWithIntercept(1)),
      "The different between newWeights with/without regularization " +
        "should be initialWeightsWithIntercept.")
  }
}
