---
layout: global
title: Basics - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Basics
---

* Table of contents
{:toc}

MLlib supports local vectors and matrices stored on a single machine, 
as well as distributed matrices backed by one or more RDDs.
In the current implementation, local vectors and matrices are simple data models 
to serve public interfaces. The underlying linear algebra operations are provided by
[Breeze](http://www.scalanlp.org/) and [jblas](http://jblas.org/).
A training example used in supervised learning is called "labeled point" in MLlib.

## Local vector

A local vector has integer-typed and 0-based indices and double-typed values, stored on a single
machine.  MLlib supports two types of local vectors: dense and sparse.  A dense vector is backed by
a double array representing its entry values, while a sparse vector is backed by two parallel
arrays: indices and values.  For example, a vector $(1.0, 0.0, 3.0)$ can be represented in dense
format as `[1.0, 0.0, 3.0]` or in sparse format as `(3, [0, 2], [1.0, 3.0])`, where `3` is the size
of the vector.

<div class="codetabs">
<div data-lang="scala" markdown="1">

The base class of local vectors is
[`Vector`](api/scala/index.html#org.apache.spark.mllib.linalg.Vector), and we provide two
implementations: [`DenseVector`](api/scala/index.html#org.apache.spark.mllib.linalg.DenseVector) and
[`SparseVector`](api/scala/index.html#org.apache.spark.mllib.linalg.SparseVector).  We recommend
using the factory methods implemented in
[`Vectors`](api/scala/index.html#org.apache.spark.mllib.linalg.Vector) to create local vectors.

{% highlight scala %}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

// Create a dense vector (1.0, 0.0, 3.0).
val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
// Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
// Create a sparse vector (1.0, 0.0, 3.0) by specifying its nonzero entries.
val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
{% endhighlight %}

***Note***

Scala imports `scala.collection.immutable.Vector` by default, so you have to import
`org.apache.spark.mllib.linalg.Vector` explicitly to use MLlib's `Vector`.

</div>

<div data-lang="java" markdown="1">

The base class of local vectors is
[`Vector`](api/java/org/apache/spark/mllib/linalg/Vector.html), and we provide two
implementations: [`DenseVector`](api/java/org/apache/spark/mllib/linalg/DenseVector.html) and
[`SparseVector`](api/java/org/apache/spark/mllib/linalg/SparseVector.html).  We recommend
using the factory methods implemented in
[`Vectors`](api/java/org/apache/spark/mllib/linalg/Vector.html) to create local vectors.

{% highlight java %}
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

// Create a dense vector (1.0, 0.0, 3.0).
Vector dv = Vectors.dense(1.0, 0.0, 3.0);
// Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
Vector sv = Vectors.sparse(3, new int[] {0, 2}, new double[] {1.0, 3.0});
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
MLlib recognizes the following types as dense vectors:

* NumPy's [`array`](http://docs.scipy.org/doc/numpy/reference/generated/numpy.array.html)
* Python's list, e.g., `[1, 2, 3]`

and the following as sparse vectors:

* MLlib's [`SparseVector`](api/python/pyspark.mllib.linalg.SparseVector-class.html).
* SciPy's
  [`csc_matrix`](http://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.csc_matrix.html#scipy.sparse.csc_matrix)
  with a single column

We recommend using NumPy arrays over lists for efficiency, and using the factory methods implemented
in [`Vectors`](api/python/pyspark.mllib.linalg.Vectors-class.html) to create sparse vectors.

{% highlight python %}
import numpy as np
import scipy.sparse as sps
from pyspark.mllib.linalg import Vectors

# Use a NumPy array as a dense vector.
dv1 = np.array([1.0, 0.0, 3.0])
# Use a Python list as a dense vector.
dv2 = [1.0, 0.0, 3.0]
# Create a SparseVector.
sv1 = Vectors.sparse(3, [0, 2], [1.0, 3.0])
# Use a single-column SciPy csc_matrix as a sparse vector.
sv2 = sps.csc_matrix((np.array([1.0, 3.0]), np.array([0, 2]), np.array([0, 2])), shape = (3, 1))
{% endhighlight %}

</div>
</div>

## Labeled point

A labeled point is a local vector, either dense or sparse, associated with a label/response.
In MLlib, labeled points are used in supervised learning algorithms.
We use a double to store a label, so we can use labeled points in both regression and classification.
For binary classification, label should be either $0$ (negative) or $1$ (positive).
For multiclass classification, labels should be class indices staring from zero: $0, 1, 2, \ldots$.

<div class="codetabs">

<div data-lang="scala" markdown="1">

A labeled point is represented by the case class
[`LabeledPoint`](api/scala/index.html#org.apache.spark.mllib.regression.LabeledPoint).

{% highlight scala %}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

// Create a labeled point with a positive label and a dense feature vector.
val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))

// Create a labeled point with a negative label and a sparse feature vector.
val neg = LabeledPoint(0.0, Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0)))
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

A labeled point is represented by
[`LabeledPoint`](api/java/org/apache/spark/mllib/regression/LabeledPoint.html).

{% highlight java %}
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

// Create a labeled point with a positive label and a dense feature vector.
LabeledPoint pos = new LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0));

// Create a labeled point with a negative label and a sparse feature vector.
LabeledPoint neg = new LabeledPoint(1.0, Vectors.sparse(3, new int[] {0, 2}, new double[] {1.0, 3.0}));
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">

A labeled point is represented by
[`LabeledPoint`](api/python/pyspark.mllib.regression.LabeledPoint-class.html).

{% highlight python %}
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint

# Create a labeled point with a positive label and a dense feature vector.
pos = LabeledPoint(1.0, [1.0, 0.0, 3.0])

# Create a labeled point with a negative label and a sparse feature vector.
neg = LabeledPoint(0.0, SparseVector(3, [0, 2], [1.0, 3.0]))
{% endhighlight %}
</div>
</div>

***Sparse data***

It is very common in practice to have sparse training data.  MLlib supports reading training
examples stored in `LIBSVM` format, which is the default format used by
[`LIBSVM`](http://www.csie.ntu.edu.tw/~cjlin/libsvm/) and
[`LIBLINEAR`](http://www.csie.ntu.edu.tw/~cjlin/liblinear/).  It is a text format.  Each line
represents a labeled sparse feature vector using the following format:

~~~
label index1:value1 index2:value2 ...
~~~

where the indices are one-based and in ascending order. 
After loading, the feature indices are converted to zero-based.

<div class="codetabs">
<div data-lang="scala" markdown="1">

[`MLUtils.loadLibSVMFile`](api/scala/index.html#org.apache.spark.mllib.util.MLUtils$) reads training
examples stored in LIBSVM format.

{% highlight scala %}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

val examples: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "mllib/data/sample_libsvm_data.txt")
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
[`MLUtils.loadLibSVMFile`](api/java/org/apache/spark/mllib/util/MLUtils.html) reads training
examples stored in LIBSVM format.

{% highlight java %}
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.api.java.JavaRDD;

JavaRDD<LabeledPoint> examples = 
  MLUtils.loadLibSVMFile(jsc.sc(), "mllib/data/sample_libsvm_data.txt").toJavaRDD();
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
[`MLUtils.loadLibSVMFile`](api/python/pyspark.mllib.util.MLUtils-class.html) reads training
examples stored in LIBSVM format.

{% highlight python %}
from pyspark.mllib.util import MLUtils

examples = MLUtils.loadLibSVMFile(sc, "mllib/data/sample_libsvm_data.txt")
{% endhighlight %}
</div>
</div>

## Local matrix

A local matrix has integer-typed row and column indices and double-typed values, stored on a single
machine.  MLlib supports dense matrix, whose entry values are stored in a single double array in
column major.  For example, the following matrix `\[ \begin{pmatrix}
1.0 & 2.0 \\
3.0 & 4.0 \\
5.0 & 6.0
\end{pmatrix}
\]`
is stored in a one-dimensional array `[1.0, 3.0, 5.0, 2.0, 4.0, 6.0]` with the matrix size `(3, 2)`.
We are going to add sparse matrix in the next release.

<div class="codetabs">
<div data-lang="scala" markdown="1">

The base class of local matrices is
[`Matrix`](api/scala/index.html#org.apache.spark.mllib.linalg.Matrix), and we provide one
implementation: [`DenseMatrix`](api/scala/index.html#org.apache.spark.mllib.linalg.DenseMatrix).
Sparse matrix will be added in the next release.  We recommend using the factory methods implemented
in [`Matrices`](api/scala/index.html#org.apache.spark.mllib.linalg.Matrices) to create local
matrices.

{% highlight scala %}
import org.apache.spark.mllib.linalg.{Matrix, Matrices}

// Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

The base class of local matrices is
[`Matrix`](api/java/org/apache/spark/mllib/linalg/Matrix.html), and we provide one
implementation: [`DenseMatrix`](api/java/org/apache/spark/mllib/linalg/DenseMatrix.html).
Sparse matrix will be added in the next release.  We recommend using the factory methods implemented
in [`Matrices`](api/java/org/apache/spark/mllib/linalg/Matrices.html) to create local
matrices.

{% highlight java %}
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Matrices;

// Create a dense matrix ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
Matrix dm = Matrices.dense(3, 2, new double[] {1.0, 3.0, 5.0, 2.0, 4.0, 6.0});
{% endhighlight %}
</div>

</div>

## Distributed matrix

A distributed matrix has long-typed row and column indices and double-typed values, stored
distributively in one or more RDDs.  It is very important to choose the right format to store large
and distributed matrices.  Converting a distributed matrix to a different format may require a
global shuffle, which is quite expensive.  We implemented three types of distributed matrices in
this release and will add more types in the future.

The basic type is called `RowMatrix`. A `RowMatrix` is a row-oriented distributed
matrix without meaningful row indices, e.g., a collection of feature vectors.
It is backed by an RDD of its rows, where each row is a local vector.
We assume that the number of columns is not huge for a `RowMatrix`.
An `IndexedRowMatrix` is similar to a `RowMatrix` but with row indices,
which can be used for identifying rows and joins.
A `CoordinateMatrix` is a distributed matrix stored in [coordinate list (COO)](https://en.wikipedia.org/wiki/Sparse_matrix) format,
backed by an RDD of its entries.

***Note***

The underlying RDDs of a distributed matrix must be deterministic, because we cache the matrix size.
It is always error-prone to have non-deterministic RDDs.

### RowMatrix

A `RowMatrix` is a row-oriented distributed matrix without meaningful row indices, backed by an RDD
of its rows, where each row is a local vector.  This is similar to `data matrix` in the context of
multivariate statistics.  Since each row is represented by a local vector, the number of columns is
limited by the integer range but it should be much smaller in practice.

<div class="codetabs">
<div data-lang="scala" markdown="1">

A [`RowMatrix`](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.RowMatrix) can be
created from an `RDD[Vector]` instance.  Then we can compute its column summary statistics.

{% highlight scala %}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.RowMatrix

val rows: RDD[Vector] = ... // an RDD of local vectors
// Create a RowMatrix from an RDD[Vector].
val mat: RowMatrix = new RowMatrix(rows)

// Get its size.
val m = mat.numRows()
val n = mat.numCols()
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

A [`RowMatrix`](api/java/org/apache/spark/mllib/linalg/distributed/RowMatrix.html) can be
created from a `JavaRDD<Vector>` instance.  Then we can compute its column summary statistics.

{% highlight java %}
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

JavaRDD<Vector> rows = ... // a JavaRDD of local vectors
// Create a RowMatrix from an JavaRDD<Vector>.
RowMatrix mat = new RowMatrix(rows.rdd());

// Get its size.
long m = mat.numRows();
long n = mat.numCols();
{% endhighlight %}
</div>
</div>

#### Multivariate summary statistics

We provide column summary statistics for `RowMatrix`. 
If the number of columns is not large, say, smaller than 3000, you can also compute
the covariance matrix as a local matrix, which requires $\mathcal{O}(n^2)$ storage where $n$ is the
number of columns. The total CPU time is $\mathcal{O}(m n^2)$, where $m$ is the number of rows,
which could be faster if the rows are sparse.

<div class="codetabs">
<div data-lang="scala" markdown="1">

[`RowMatrix#computeColumnSummaryStatistics`](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.RowMatrix) returns an instance of
[`MultivariateStatisticalSummary`](api/scala/index.html#org.apache.spark.mllib.stat.MultivariateStatisticalSummary),
which contains the column-wise max, min, mean, variance, and number of nonzeros, as well as the
total count.

{% highlight scala %}
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary

val mat: RowMatrix = ... // a RowMatrix

// Compute column summary statistics.
val summary: MultivariateStatisticalSummary = mat.computeColumnSummaryStatistics()
println(summary.mean) // a dense vector containing the mean value for each column
println(summary.variance) // column-wise variance
println(summary.numNonzeros) // number of nonzeros in each column

// Compute the covariance matrix.
val cov: Matrix = mat.computeCovariance()
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

[`RowMatrix#computeColumnSummaryStatistics`](api/java/org/apache/spark/mllib/linalg/distributed/RowMatrix.html#computeColumnSummaryStatistics()) returns an instance of
[`MultivariateStatisticalSummary`](api/java/org/apache/spark/mllib/stat/MultivariateStatisticalSummary.html),
which contains the column-wise max, min, mean, variance, and number of nonzeros, as well as the
total count.

{% highlight java %}
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;

RowMatrix mat = ... // a RowMatrix

// Compute column summary statistics.
MultivariateStatisticalSummary summary = mat.computeColumnSummaryStatistics();
System.out.println(summary.mean()); // a dense vector containing the mean value for each column
System.out.println(summary.variance()); // column-wise variance
System.out.println(summary.numNonzeros()); // number of nonzeros in each column

// Compute the covariance matrix.
Matrix cov = mat.computeCovariance();
{% endhighlight %}
</div>
</div>

### IndexedRowMatrix

An `IndexedRowMatrix` is similar to a `RowMatrix` but with meaningful row indices.  It is backed by
an RDD of indexed rows, which each row is represented by its index (long-typed) and a local vector.

<div class="codetabs">
<div data-lang="scala" markdown="1">

An
[`IndexedRowMatrix`](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix)
can be created from an `RDD[IndexedRow]` instance, where
[`IndexedRow`](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.IndexedRow) is a
wrapper over `(Long, Vector)`.  An `IndexedRowMatrix` can be converted to a `RowMatrix` by dropping
its row indices.

{% highlight scala %}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}

val rows: RDD[IndexedRow] = ... // an RDD of indexed rows
// Create an IndexedRowMatrix from an RDD[IndexedRow].
val mat: IndexedRowMatrix = new IndexedRowMatrix(rows)

// Get its size.
val m = mat.numRows()
val n = mat.numCols()

// Drop its row indices.
val rowMat: RowMatrix = mat.toRowMatrix()
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

An
[`IndexedRowMatrix`](api/java/org/apache/spark/mllib/linalg/distributed/IndexedRowMatrix.html)
can be created from an `JavaRDD<IndexedRow>` instance, where
[`IndexedRow`](api/java/org/apache/spark/mllib/linalg/distributed/IndexedRow.html) is a
wrapper over `(long, Vector)`.  An `IndexedRowMatrix` can be converted to a `RowMatrix` by dropping
its row indices.

{% highlight java %}
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

JavaRDD<IndexedRow> rows = ... // a JavaRDD of indexed rows
// Create an IndexedRowMatrix from a JavaRDD<IndexedRow>.
IndexedRowMatrix mat = new IndexedRowMatrix(rows.rdd());

// Get its size.
long m = mat.numRows();
long n = mat.numCols();

// Drop its row indices.
RowMatrix rowMat = mat.toRowMatrix();
{% endhighlight %}
</div></div>

### CoordinateMatrix

A `CoordinateMatrix` is a distributed matrix backed by an RDD of its entries.  Each entry is a tuple
of `(i: Long, j: Long, value: Double)`, where `i` is the row index, `j` is the column index, and
`value` is the entry value.  A `CoordinateMatrix` should be used only in the case when both
dimensions of the matrix are huge and the matrix is very sparse.

<div class="codetabs">
<div data-lang="scala" markdown="1">

A
[`CoordinateMatrix`](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.CoordinateMatrix)
can be created from an `RDD[MatrixEntry]` instance, where
[`MatrixEntry`](api/scala/index.html#org.apache.spark.mllib.linalg.distributed.MatrixEntry) is a
wrapper over `(Long, Long, Double)`.  A `CoordinateMatrix` can be converted to a `IndexedRowMatrix`
with sparse rows by calling `toIndexedRowMatrix`.  In this release, we do not provide other
computation for `CoordinateMatrix`.

{% highlight scala %}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}

val entries: RDD[MatrixEntry] = ... // an RDD of matrix entries
// Create a CoordinateMatrix from an RDD[MatrixEntry].
val mat: CoordinateMatrix = new CoordinateMatrix(entries)

// Get its size.
val m = mat.numRows()
val n = mat.numCols()

// Convert it to an IndexRowMatrix whose rows are sparse vectors.
val indexedRowMatrix = mat.toIndexedRowMatrix()
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">

A
[`CoordinateMatrix`](api/java/org/apache/spark/mllib/linalg/distributed/CoordinateMatrix.html)
can be created from a `JavaRDD<MatrixEntry>` instance, where
[`MatrixEntry`](api/java/org/apache/spark/mllib/linalg/distributed/MatrixEntry.html) is a
wrapper over `(long, long, double)`.  A `CoordinateMatrix` can be converted to a `IndexedRowMatrix`
with sparse rows by calling `toIndexedRowMatrix`.

{% highlight java %}
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;

JavaRDD<MatrixEntry> entries = ... // a JavaRDD of matrix entries
// Create a CoordinateMatrix from a JavaRDD<MatrixEntry>.
CoordinateMatrix mat = new CoordinateMatrix(entries.rdd());

// Get its size.
long m = mat.numRows();
long n = mat.numCols();

// Convert it to an IndexRowMatrix whose rows are sparse vectors.
IndexedRowMatrix indexedRowMatrix = mat.toIndexedRowMatrix();
{% endhighlight %}
</div>
</div>
