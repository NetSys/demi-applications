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

import com.typesafe.tools.mima.core._

/**
 * Additional excludes for checking of Spark's binary compatibility.
 *
 * The Mima build will automatically exclude @DeveloperApi and @Experimental classes. This acts
 * as an official audit of cases where we excluded other classes. Please use the narrowest
 * possible exclude here. MIMA will usually tell you what exclude to use, e.g.:
 *
 * ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.rdd.RDD.take")
 *
 * It is also possible to exclude Spark classes and packages. This should be used sparingly:
 *
 * MimaBuild.excludeSparkClass("graphx.util.collection.GraphXPrimitiveKeyOpenHashMap")
 */
object MimaExcludes {
    val excludes =
      SparkBuild.SPARK_VERSION match {
        case v if v.startsWith("1.1") =>
          Seq(
            MimaBuild.excludeSparkPackage("deploy"),
            MimaBuild.excludeSparkPackage("graphx")
          ) ++
          Seq(
            // Adding new method to JavaRDLike trait - we should probably mark this as a developer API.
            ProblemFilters.exclude[MissingMethodProblem]("org.apache.spark.api.java.JavaRDDLike.partitions"),
            // We made a mistake earlier (ed06500d3) in the Java API to use default parameter values
            // for countApproxDistinct* functions, which does not work in Java. We later removed
            // them, and use the following to tell Mima to not care about them.
            ProblemFilters.exclude[IncompatibleResultTypeProblem](
              "org.apache.spark.api.java.JavaPairRDD.countApproxDistinctByKey"),
            ProblemFilters.exclude[IncompatibleResultTypeProblem](
              "org.apache.spark.api.java.JavaPairRDD.countApproxDistinctByKey"),
            ProblemFilters.exclude[MissingMethodProblem](
              "org.apache.spark.api.java.JavaPairRDD.countApproxDistinct$default$1"),
            ProblemFilters.exclude[MissingMethodProblem](
              "org.apache.spark.api.java.JavaPairRDD.countApproxDistinctByKey$default$1"),
            ProblemFilters.exclude[MissingMethodProblem](
              "org.apache.spark.api.java.JavaRDD.countApproxDistinct$default$1"),
            ProblemFilters.exclude[MissingMethodProblem](
              "org.apache.spark.api.java.JavaRDDLike.countApproxDistinct$default$1"),
            ProblemFilters.exclude[MissingMethodProblem](
              "org.apache.spark.api.java.JavaDoubleRDD.countApproxDistinct$default$1"),
            ProblemFilters.exclude[MissingMethodProblem](
              "org.apache.spark.storage.MemoryStore.Entry"),
            ProblemFilters.exclude[MissingMethodProblem](
              "org.apache.spark.rdd.PairRDDFunctions.org$apache$spark$rdd$PairRDDFunctions$$"
                + "createZero$1")
          ) ++
          Seq( // Ignore some private methods in ALS.
            ProblemFilters.exclude[MissingMethodProblem](
              "org.apache.spark.mllib.recommendation.ALS.org$apache$spark$mllib$recommendation$ALS$^dateFeatures"),
            ProblemFilters.exclude[MissingMethodProblem]( // The only public constructor is the one without arguments.
              "org.apache.spark.mllib.recommendation.ALS.this"),
            ProblemFilters.exclude[MissingMethodProblem](
              "org.apache.spark.mllib.recommendation.ALS.org$apache$spark$mllib$recommendation$ALS$$<init>$default$7")
          ) ++
          MimaBuild.excludeSparkClass("rdd.ZippedRDD") ++
          MimaBuild.excludeSparkClass("rdd.ZippedPartition") ++
          MimaBuild.excludeSparkClass("util.SerializableHyperLogLog") ++
          MimaBuild.excludeSparkClass("storage.Values") ++
          MimaBuild.excludeSparkClass("storage.Entry") ++
          MimaBuild.excludeSparkClass("storage.MemoryStore$Entry")
        case v if v.startsWith("1.0") =>
          Seq(
            MimaBuild.excludeSparkPackage("api.java"),
            MimaBuild.excludeSparkPackage("mllib"),
            MimaBuild.excludeSparkPackage("streaming")
          ) ++
          MimaBuild.excludeSparkClass("rdd.ClassTags") ++
          MimaBuild.excludeSparkClass("util.XORShiftRandom") ++
          MimaBuild.excludeSparkClass("graphx.EdgeRDD") ++
          MimaBuild.excludeSparkClass("graphx.VertexRDD") ++
          MimaBuild.excludeSparkClass("graphx.impl.GraphImpl") ++
          MimaBuild.excludeSparkClass("graphx.impl.RoutingTable") ++
          MimaBuild.excludeSparkClass("graphx.util.collection.PrimitiveKeyOpenHashMap") ++
          MimaBuild.excludeSparkClass("graphx.util.collection.GraphXPrimitiveKeyOpenHashMap") ++
          MimaBuild.excludeSparkClass("mllib.recommendation.MFDataGenerator") ++
          MimaBuild.excludeSparkClass("mllib.optimization.SquaredGradient") ++
          MimaBuild.excludeSparkClass("mllib.regression.RidgeRegressionWithSGD") ++
          MimaBuild.excludeSparkClass("mllib.regression.LassoWithSGD") ++
          MimaBuild.excludeSparkClass("mllib.regression.LinearRegressionWithSGD")
        case _ => Seq()
      }
}
