---
layout: global
title: Linear Methods - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Linear Methods
---

* Table of contents
{:toc}


`\[
\newcommand{\R}{\mathbb{R}}
\newcommand{\E}{\mathbb{E}} 
\newcommand{\x}{\mathbf{x}}
\newcommand{\y}{\mathbf{y}}
\newcommand{\wv}{\mathbf{w}}
\newcommand{\av}{\mathbf{\alpha}}
\newcommand{\bv}{\mathbf{b}}
\newcommand{\N}{\mathbb{N}}
\newcommand{\id}{\mathbf{I}} 
\newcommand{\ind}{\mathbf{1}} 
\newcommand{\0}{\mathbf{0}} 
\newcommand{\unit}{\mathbf{e}} 
\newcommand{\one}{\mathbf{1}} 
\newcommand{\zero}{\mathbf{0}}
\]`

## Mathematical formulation

Many standard *machine learning* methods can be formulated as a convex optimization problem, i.e.
the task of finding a minimizer of a convex function `$f$` that depends on a variable vector
`$\wv$` (called `weights` in the code), which has `$d$` entries. 
Formally, we can write this as the optimization problem `$\min_{\wv \in\R^d} \; f(\wv)$`, where
the objective function is of the form
`\begin{equation}
    f(\wv) := 
    \frac1n \sum_{i=1}^n L(\wv;\x_i,y_i) +
    \lambda\, R(\wv_i)
    \label{eq:regPrimal}
    \ .
\end{equation}`
Here the vectors `$\x_i\in\R^d$` are the training data examples, for `$1\le i\le n$`, and
`$y_i\in\R$` are their corresponding labels, which we want to predict. 
We call the method *linear* if $L(\wv; \x, y)$ can be expressed as a function of $\wv^T x$ and $y$.
Several MLlib's classification and regression algorithms fall into this category,
and are discussed here.

The objective function `$f$` has two parts:
the loss that measures the error of the model on the training data, 
and the regularizer that measures the complexity of the model.
The loss function `$L(\wv;.)$` must be a convex function in `$\wv$`.
The fixed regularization parameter `$\lambda \ge 0$` (`regParam` in the code) defines the trade-off
between the two goals of small loss and small model complexity.

### Loss functions

The following table summarizes the loss functions and their gradients or sub-gradients for the
methods MLlib supports:

<table class="table">
  <thead>
    <tr><th></th><th>loss function $L(\wv; \x, y)$</th><th>gradient or sub-gradient</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>hinge loss</td><td>$\max \{0, 1-y \wv^T \x \}, \quad y \in \{-1, +1\}$</td>
      <td>$\begin{cases}-y \cdot \x &amp; \text{if $y \wv^T \x &lt;1$}, \\ 0 &amp;
\text{otherwise}.\end{cases}$</td>
    </tr>
    <tr>
      <td>logistic loss</td><td>$\log(1+\exp( -y \wv^T \x)), \quad y \in \{-1, +1\}$</td>
      <td>$-y \left(1-\frac1{1+\exp(-y \wv^T \x)} \right) \cdot \x$</td>
    </tr>
    <tr>
      <td>squared loss</td><td>$\frac{1}{2} (\wv^T \x - y)^2, \quad y \in \R$</td>
      <td>$(\wv^T \x - y) \cdot \x$</td>
    </tr>
  </tbody>
</table>

### Regularizers

The purpose of the [regularizer](http://en.wikipedia.org/wiki/Regularization_(mathematics)) is to
encourage simple models, by punishing the complexity of the model `$\wv$`, in order to e.g. avoid
over-fitting.
We support the following regularizers in MLlib:

<table class="table">
  <thead>
    <tr><th></th><th>regularizer $R(\wv)$</th><th>gradient or sub-gradient</th></tr>
  </thead>
  <tbody>
    <tr>
      <td>zero (unregularized)</td><td>0</td><td>$\0$</td>
    </tr>
    <tr>
      <td>L2</td><td>$\frac{1}{2}\|\wv\|_2^2$</td><td>$\wv$</td>
    </tr>
    <tr>
      <td>L1</td><td>$\|\wv\|_1$</td><td>$\mathrm{sign}(\wv)$</td>
    </tr>
  </tbody>
</table>

Here `$\mathrm{sign}(\wv)$` is the vector consisting of the signs (`$\pm1$`) of all the entries
of `$\wv$`.

L2-regularized problems are generally easier to solve than L1-regularized due to smoothness.
However, L1 regularization can help promote sparsity in weights, leading to simpler models, which is
also used for feature selection.  It is not recommended to train models without any regularization,
especially when the number of training examples is small.

## Binary classification

[Binary classification](http://en.wikipedia.org/wiki/Binary_classification) is to divide items into
two categories: positive and negative.  MLlib supports two linear methods for binary classification:
linear support vector machine (SVM) and logistic regression.  The training data set is represented
by an RDD of [LabeledPoint](mllib-data-types.html) in MLlib.  Note that, in the mathematical
formulation, a training label $y$ is either $+1$ (positive) or $-1$ (negative), which is convenient
for the formulation.  *However*, the negative label is represented by $0$ in MLlib instead of $-1$,
to be consistent with multiclass labeling.

### Linear support vector machine (SVM)

The [linear SVM](http://en.wikipedia.org/wiki/Support_vector_machine#Linear_SVM)
has become a standard choice for large-scale classification tasks.
The name "linear SVM" is actually ambiguous.
By "linear SVM", we mean specifically the linear method with the loss function in formulation
`$\eqref{eq:regPrimal}$` given by the hinge loss
`\[
L(\wv;\x,y) := \max \{0, 1-y \wv^T \x \}.
\]`
By default, linear SVMs are trained with an L2 regularization.
We also support alternative L1 regularization. In this case,
the problem becomes a [linear program](http://en.wikipedia.org/wiki/Linear_programming).

Linear SVM algorithm outputs a SVM model, which makes predictions based on the value of $\wv^T \x$.
By the default, if $\wv^T \x \geq 0$, the outcome is positive, or negative otherwise.
However, quite often in practice, the default threshold $0$ is not a good choice.
The threshold should be determined via model evaluation.

### Logistic regression

[Logistic regression](http://en.wikipedia.org/wiki/Logistic_regression) is widely used to predict a
binary response.  It is a linear method with the loss function in formulation
`$\eqref{eq:regPrimal}$` given by the logistic loss
`\[
L(\wv;\x,y) :=  \log(1+\exp( -y \wv^T \x)).
\]`

Logistic regression algorithm outputs a logistic regression model, which makes predictions by
applying the logistic function
`\[
\mathrm{logit}(z) = \frac{1}{1 + e^{-z}}
\]`
$\wv^T \x$.
By default, if $\mathrm{logit}(\wv^T x) > 0.5$, the outcome is positive, or negative otherwise.
For the same reason mentioned above, quite often in practice, this default threshold is not a good choice.
The threshold should be determined via model evaluation.

### Evaluation metrics

MLlib supports common evaluation metrics for binary classification (not available in Python).  This
includes precision, recall, [F-measure](http://en.wikipedia.org/wiki/F1_score),
[receiver operating characteristic (ROC)](http://en.wikipedia.org/wiki/Receiver_operating_characteristic),
precision-recall curve, and
[area under the curves (AUC)](http://en.wikipedia.org/wiki/Receiver_operating_characteristic#Area_under_the_curve).
Among the metrics, area under ROC is commonly used to compare models and precision/recall/F-measure
can help determine the threshold to use.

### Examples

<div class="codetabs">

<div data-lang="scala" markdown="1">
The following code snippet illustrates how to load a sample dataset, execute a
training algorithm on this training data using a static method in the algorithm
object, and make predictions with the resulting model to compute the training
error.

{% highlight scala %}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils

// Load training data in LIBSVM format.
val data = MLUtils.loadLibSVMFile(sc, "mllib/data/sample_libsvm_data.txt")

// Split data into training (60%) and test (40%).
val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
val training = splits(0).cache()
val test = splits(1)

// Run training algorithm to build the model
val numIterations = 100
val model = SVMWithSGD.train(training, numIterations)

// Clear the default threshold.
model.clearThreshold()

// Compute raw scores on the test set. 
val scoreAndLabels = test.map { point =>
  val score = model.predict(point.features)
  (score, point.label)
}

// Get evaluation metrics.
val metrics = new BinaryClassificationMetrics(scoreAndLabels)
val auROC = metrics.areaUnderROC()

println("Area under ROC = " + auROC)
{% endhighlight %}

The `SVMWithSGD.train()` method by default performs L2 regularization with the
regularization parameter set to 1.0. If we want to configure this algorithm, we
can customize `SVMWithSGD` further by creating a new object directly and
calling setter methods. All other MLlib algorithms support customization in
this way as well. For example, the following code produces an L1 regularized
variant of SVMs with regularization parameter set to 0.1, and runs the training
algorithm for 200 iterations.

{% highlight scala %}
import org.apache.spark.mllib.optimization.L1Updater

val svmAlg = new SVMWithSGD()
svmAlg.optimizer.
  setNumIterations(200).
  setRegParam(0.1).
  setUpdater(new L1Updater)
val modelL1 = svmAlg.run(training)
{% endhighlight %}

Similarly, you can use replace `SVMWithSGD` by
[`LogisticRegressionWithSGD`](api/scala/index.html#org.apache.spark.mllib.classification.LogisticRegressionWithSGD).

</div>

<div data-lang="java" markdown="1">
All of MLlib's methods use Java-friendly types, so you can import and call them there the same
way you do in Scala. The only caveat is that the methods take Scala RDD objects, while the
Spark Java API uses a separate `JavaRDD` class. You can convert a Java RDD to a Scala one by
calling `.rdd()` on your `JavaRDD` object.
</div>

<div data-lang="python" markdown="1">
The following example shows how to load a sample dataset, build Logistic Regression model,
and make predictions with the resulting model to compute the training error.

{% highlight python %}
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint
from numpy import array

# Load and parse the data
def parsePoint(line):
    values = [float(x) for x in line.split(' ')]
    return LabeledPoint(values[0], values[1:])

data = sc.textFile("mllib/data/sample_svm_data.txt")
parsedData = data.map(parsePoint)

# Build the model
model = LogisticRegressionWithSGD.train(parsedData)

# Evaluating the model on training data
labelsAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(parsedData.count())
print("Training Error = " + str(trainErr))
{% endhighlight %}
</div>
</div>

## Linear least squares, Lasso, and ridge regression

Linear least squares is a family of linear methods with the loss function in formulation
`$\eqref{eq:regPrimal}$` given by the squared loss

`\[
L(\wv;\x,y) :=  \frac{1}{2} (\wv^T \x - y)^2.
\]`

Depending on the regularization type, we call the method
[*ordinary least squares*](http://en.wikipedia.org/wiki/Ordinary_least_squares) or simply
[*linear least squares*](http://en.wikipedia.org/wiki/Linear_least_squares_(mathematics)) if there
is no regularization, [*ridge regression*](http://en.wikipedia.org/wiki/Ridge_regression) if L2
regularization is used, and [*Lasso*](http://en.wikipedia.org/wiki/Lasso_(statistics)) if L1
regularization is used.  This average loss $\frac{1}{n} \sum_{i=1}^n (\wv^T x_i - y_i)^2$ is also
known as the [mean squared error](http://en.wikipedia.org/wiki/Mean_squared_error).

Note that the squared loss is sensitive to outliers. 
Regularization or a robust alternative (e.g., $\ell_1$ regression) is usually necessary in practice.

### Examples

<div class="codetabs">

<div data-lang="scala" markdown="1">
The following example demonstrate how to load training data, parse it as an RDD of LabeledPoint.
The example then uses LinearRegressionWithSGD to build a simple linear model to predict label 
values. We compute the Mean Squared Error at the end to evaluate
[goodness of fit](http://en.wikipedia.org/wiki/Goodness_of_fit).

{% highlight scala %}
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

// Load and parse the data
val data = sc.textFile("mllib/data/ridge-data/lpsa.data")
val parsedData = data.map { line =>
  val parts = line.split(',')
  LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
}

// Building the model
val numIterations = 100
val model = LinearRegressionWithSGD.train(parsedData, numIterations)

// Evaluate model on training examples and compute training error
val valuesAndPreds = parsedData.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
println("training Mean Squared Error = " + MSE)
{% endhighlight %}

Similarly you can use
[`RidgeRegressionWithSGD`](api/scala/index.html#org.apache.spark.mllib.regression.RidgeRegressionWithSGD)
and [`LassoWithSGD`](api/scala/index.html#org.apache.spark.mllib.regression.LassoWithSGD).

</div>

<div data-lang="java" markdown="1">
All of MLlib's methods use Java-friendly types, so you can import and call them there the same
way you do in Scala. The only caveat is that the methods take Scala RDD objects, while the
Spark Java API uses a separate `JavaRDD` class. You can convert a Java RDD to a Scala one by
calling `.rdd()` on your `JavaRDD` object.
</div>

<div data-lang="python" markdown="1">
The following example demonstrate how to load training data, parse it as an RDD of LabeledPoint.
The example then uses LinearRegressionWithSGD to build a simple linear model to predict label 
values. We compute the Mean Squared Error at the end to evaluate
[goodness of fit](http://en.wikipedia.org/wiki/Goodness_of_fit).

{% highlight python %}
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
from numpy import array

# Load and parse the data
def parsePoint(line):
    values = [float(x) for x in line.replace(',', ' ').split(' ')]
    return LabeledPoint(values[0], values[1:])

data = sc.textFile("mllib/data/ridge-data/lpsa.data")
parsedData = data.map(parsePoint)

# Build the model
model = LinearRegressionWithSGD.train(parsedData)

# Evaluate the model on training data
valuesAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
MSE = valuesAndPreds.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y) / valuesAndPreds.count()
print("Mean Squared Error = " + str(MSE))
{% endhighlight %}
</div>
</div>

## Implementation (developer)

Behind the scene, MLlib implements a simple distributed version of stochastic gradient descent
(SGD), building on the underlying gradient descent primitive (as described in the <a
href="mllib-optimization.html">optimization</a> section).  All provided algorithms take as input a
regularization parameter (`regParam`) along with various parameters associated with stochastic
gradient descent (`stepSize`, `numIterations`, `miniBatchFraction`).  For each of them, we support
all three possible regularizations (none, L1 or L2).

Algorithms are all implemented in Scala:

* [SVMWithSGD](api/scala/index.html#org.apache.spark.mllib.classification.SVMWithSGD)
* [LogisticRegressionWithSGD](api/scala/index.html#org.apache.spark.mllib.classification.LogisticRegressionWithSGD)
* [LinearRegressionWithSGD](api/scala/index.html#org.apache.spark.mllib.regression.LinearRegressionWithSGD)
* [RidgeRegressionWithSGD](api/scala/index.html#org.apache.spark.mllib.regression.RidgeRegressionWithSGD)
* [LassoWithSGD](api/scala/index.html#org.apache.spark.mllib.regression.LassoWithSGD)

Python calls the Scala implementation via
[PythonMLLibAPI](api/scala/index.html#org.apache.spark.mllib.api.python.PythonMLLibAPI).
