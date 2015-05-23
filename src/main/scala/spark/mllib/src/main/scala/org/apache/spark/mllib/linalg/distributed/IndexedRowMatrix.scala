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

package org.apache.spark.mllib.linalg.distributed

import breeze.linalg.{DenseMatrix => BDM}

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.SingularValueDecomposition

/**
 * :: Experimental ::
 * Represents a row of [[org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix]].
 */
@Experimental
case class IndexedRow(index: Long, vector: Vector)

/**
 * :: Experimental ::
 * Represents a row-oriented [[org.apache.spark.mllib.linalg.distributed.DistributedMatrix]] with
 * indexed rows.
 *
 * @param rows indexed rows of this matrix
 * @param nRows number of rows. A non-positive value means unknown, and then the number of rows will
 *              be determined by the max row index plus one.
 * @param nCols number of columns. A non-positive value means unknown, and then the number of
 *              columns will be determined by the size of the first row.
 */
@Experimental
class IndexedRowMatrix(
    val rows: RDD[IndexedRow],
    private var nRows: Long,
    private var nCols: Int) extends DistributedMatrix {

  /** Alternative constructor leaving matrix dimensions to be determined automatically. */
  def this(rows: RDD[IndexedRow]) = this(rows, 0L, 0)

  override def numCols(): Long = {
    if (nCols <= 0) {
      // Calling `first` will throw an exception if `rows` is empty.
      nCols = rows.first().vector.size
    }
    nCols
  }

  override def numRows(): Long = {
    if (nRows <= 0L) {
      // Reduce will throw an exception if `rows` is empty.
      nRows = rows.map(_.index).reduce(math.max) + 1L
    }
    nRows
  }

  /**
   * Drops row indices and converts this matrix to a
   * [[org.apache.spark.mllib.linalg.distributed.RowMatrix]].
   */
  def toRowMatrix(): RowMatrix = {
    new RowMatrix(rows.map(_.vector), 0L, nCols)
  }

  /**
   * Computes the singular value decomposition of this matrix.
   * Denote this matrix by A (m x n), this will compute matrices U, S, V such that A = U * S * V'.
   *
   * There is no restriction on m, but we require `n^2` doubles to fit in memory.
   * Further, n should be less than m.

   * The decomposition is computed by first computing A'A = V S^2 V',
   * computing svd locally on that (since n x n is small), from which we recover S and V.
   * Then we compute U via easy matrix multiplication as U =  A * (V * S^-1).
   * Note that this approach requires `O(n^3)` time on the master node.
   *
   * At most k largest non-zero singular values and associated vectors are returned.
   * If there are k such values, then the dimensions of the return will be:
   *
   * U is an [[org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix]] of size m x k that
   * satisfies U'U = eye(k),
   * s is a Vector of size k, holding the singular values in descending order,
   * and V is a local Matrix of size n x k that satisfies V'V = eye(k).
   *
   * @param k number of singular values to keep. We might return less than k if there are
   *          numerically zero singular values. See rCond.
   * @param computeU whether to compute U
   * @param rCond the reciprocal condition number. All singular values smaller than rCond * sigma(0)
   *              are treated as zero, where sigma(0) is the largest singular value.
   * @return SingularValueDecomposition(U, s, V)
   */
  def computeSVD(
      k: Int,
      computeU: Boolean = false,
      rCond: Double = 1e-9): SingularValueDecomposition[IndexedRowMatrix, Matrix] = {
    val indices = rows.map(_.index)
    val svd = toRowMatrix().computeSVD(k, computeU, rCond)
    val U = if (computeU) {
      val indexedRows = indices.zip(svd.U.rows).map { case (i, v) =>
        IndexedRow(i, v)
      }
      new IndexedRowMatrix(indexedRows, nRows, nCols)
    } else {
      null
    }
    SingularValueDecomposition(U, svd.s, svd.V)
  }

  /**
   * Multiply this matrix by a local matrix on the right.
   *
   * @param B a local matrix whose number of rows must match the number of columns of this matrix
   * @return an IndexedRowMatrix representing the product, which preserves partitioning
   */
  def multiply(B: Matrix): IndexedRowMatrix = {
    val mat = toRowMatrix().multiply(B)
    val indexedRows = rows.map(_.index).zip(mat.rows).map { case (i, v) =>
      IndexedRow(i, v)
    }
    new IndexedRowMatrix(indexedRows, nRows, nCols)
  }

  /**
   * Computes the Gramian matrix `A^T A`.
   */
  def computeGramianMatrix(): Matrix = {
    toRowMatrix().computeGramianMatrix()
  }

  private[mllib] override def toBreeze(): BDM[Double] = {
    val m = numRows().toInt
    val n = numCols().toInt
    val mat = BDM.zeros[Double](m, n)
    rows.collect().foreach { case IndexedRow(rowIndex, vector) =>
      val i = rowIndex.toInt
      vector.toBreeze.activeIterator.foreach { case (j, v) =>
        mat(i, j) = v
      }
    }
    mat
  }
}
