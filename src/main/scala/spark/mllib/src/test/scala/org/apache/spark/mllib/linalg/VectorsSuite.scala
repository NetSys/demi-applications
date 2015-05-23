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

package org.apache.spark.mllib.linalg

import org.scalatest.FunSuite

import org.apache.spark.SparkException

class VectorsSuite extends FunSuite {

  val arr = Array(0.1, 0.0, 0.3, 0.4)
  val n = 4
  val indices = Array(0, 2, 3)
  val values = Array(0.1, 0.3, 0.4)

  test("dense vector construction with varargs") {
    val vec = Vectors.dense(arr).asInstanceOf[DenseVector]
    assert(vec.size === arr.length)
    assert(vec.values.eq(arr))
  }

  test("dense vector construction from a double array") {
   val vec = Vectors.dense(arr).asInstanceOf[DenseVector]
    assert(vec.size === arr.length)
    assert(vec.values.eq(arr))
  }

  test("sparse vector construction") {
    val vec = Vectors.sparse(n, indices, values).asInstanceOf[SparseVector]
    assert(vec.size === n)
    assert(vec.indices.eq(indices))
    assert(vec.values.eq(values))
  }

  test("sparse vector construction with unordered elements") {
    val vec = Vectors.sparse(n, indices.zip(values).reverse).asInstanceOf[SparseVector]
    assert(vec.size === n)
    assert(vec.indices === indices)
    assert(vec.values === values)
  }

  test("dense to array") {
    val vec = Vectors.dense(arr).asInstanceOf[DenseVector]
    assert(vec.toArray.eq(arr))
  }

  test("sparse to array") {
    val vec = Vectors.sparse(n, indices, values).asInstanceOf[SparseVector]
    assert(vec.toArray === arr)
  }

  test("vector equals") {
    val dv1 = Vectors.dense(arr.clone())
    val dv2 = Vectors.dense(arr.clone())
    val sv1 = Vectors.sparse(n, indices.clone(), values.clone())
    val sv2 = Vectors.sparse(n, indices.clone(), values.clone())

    val vectors = Seq(dv1, dv2, sv1, sv2)

    for (v <- vectors; u <- vectors) {
      assert(v === u)
      assert(v.## === u.##)
    }

    val another = Vectors.dense(0.1, 0.2, 0.3, 0.4)

    for (v <- vectors) {
      assert(v != another)
      assert(v.## != another.##)
    }
  }

  test("indexing dense vectors") {
    val vec = Vectors.dense(1.0, 2.0, 3.0, 4.0)
    assert(vec(0) === 1.0)
    assert(vec(3) === 4.0)
  }

  test("indexing sparse vectors") {
    val vec = Vectors.sparse(7, Array(0, 2, 4, 6), Array(1.0, 2.0, 3.0, 4.0))
    assert(vec(0) === 1.0)
    assert(vec(1) === 0.0)
    assert(vec(2) === 2.0)
    assert(vec(3) === 0.0)
    assert(vec(6) === 4.0)
    val vec2 = Vectors.sparse(8, Array(0, 2, 4, 6), Array(1.0, 2.0, 3.0, 4.0))
    assert(vec2(6) === 4.0)
    assert(vec2(7) === 0.0)
  }

  test("parse vectors") {
    val vectors = Seq(
      Vectors.dense(Array.empty[Double]),
      Vectors.dense(1.0),
      Vectors.dense(1.0E6, 0.0, -2.0e-7),
      Vectors.sparse(0, Array.empty[Int], Array.empty[Double]),
      Vectors.sparse(1, Array(0), Array(1.0)),
      Vectors.sparse(3, Array(0, 2), Array(1.0, -2.0)))
    vectors.foreach { v =>
      val v1 = Vectors.parse(v.toString)
      assert(v.getClass === v1.getClass)
      assert(v === v1)
    }

    val malformatted = Seq("1", "[1,,]", "[1,2b]", "(1,[1,2])", "([1],[2.0,1.0])")
    malformatted.foreach { s =>
      intercept[SparkException] {
        Vectors.parse(s)
        println(s"Didn't detect malformatted string $s.")
      }
    }
  }
}
