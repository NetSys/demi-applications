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

package org.apache.spark.rdd

import java.io.EOFException

import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.Reporter
import org.apache.hadoop.util.ReflectionUtils

import org.apache.spark.{Logging, Partition, SerializableWritable, SparkContext, SparkEnv, TaskContext}
import org.apache.spark.util.NextIterator
import org.apache.hadoop.conf.{Configuration, Configurable}


/**
 * A Spark split class that wraps around a Hadoop InputSplit.
 */
private[spark] class HadoopPartition(rddId: Int, idx: Int, @transient s: InputSplit)
  extends Partition {
  
  val inputSplit = new SerializableWritable[InputSplit](s)

  override def hashCode(): Int = (41 * (41 + rddId) + idx).toInt

  override val index: Int = idx
}

/**
 * An RDD that reads a Hadoop dataset as specified by a JobConf (e.g. files in HDFS, the local file
 * system, or S3, tables in HBase, etc).
 */
class HadoopRDD[K, V](
    sc: SparkContext,
    @transient conf: JobConf,
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minSplits: Int)
  extends RDD[(K, V)](sc, Nil) with Logging {

  // A Hadoop JobConf can be about 10 KB, which is pretty big, so broadcast it
  private val confBroadcast = sc.broadcast(new SerializableWritable(conf))

  override def getPartitions: Array[Partition] = {
    val env = SparkEnv.get
    env.hadoop.addCredentials(conf)
    val inputFormat = createInputFormat(conf)
    if (inputFormat.isInstanceOf[Configurable]) {
      inputFormat.asInstanceOf[Configurable].setConf(conf)
    }
    val inputSplits = inputFormat.getSplits(conf, minSplits)
    val array = new Array[Partition](inputSplits.size)
    for (i <- 0 until inputSplits.size) {
      array(i) = new HadoopPartition(id, i, inputSplits(i))
    }
    array
  }

  def createInputFormat(conf: JobConf): InputFormat[K, V] = {
    ReflectionUtils.newInstance(inputFormatClass.asInstanceOf[Class[_]], conf)
      .asInstanceOf[InputFormat[K, V]]
  }

  override def compute(theSplit: Partition, context: TaskContext) = new NextIterator[(K, V)] {
    val split = theSplit.asInstanceOf[HadoopPartition]
    logInfo("Input split: " + split.inputSplit)
    var reader: RecordReader[K, V] = null

    val conf = confBroadcast.value.value
    val fmt = createInputFormat(conf)
    if (fmt.isInstanceOf[Configurable]) {
      fmt.asInstanceOf[Configurable].setConf(conf)
    }
    reader = fmt.getRecordReader(split.inputSplit.value, conf, Reporter.NULL)

    // Register an on-task-completion callback to close the input stream.
    context.addOnCompleteCallback{ () => closeIfNeeded() }

    val key: K = reader.createKey()
    val value: V = reader.createValue()

    override def getNext() = {
      try {
        finished = !reader.next(key, value)
      } catch {
        case eof: EOFException =>
          finished = true
      }
      (key, value)
    }

    override def close() {
      try {
        reader.close()
      } catch {
        case e: Exception => logWarning("Exception in RecordReader.close()", e)
      }
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    // TODO: Filtering out "localhost" in case of file:// URLs
    val hadoopSplit = split.asInstanceOf[HadoopPartition]
    hadoopSplit.inputSplit.value.getLocations.filter(_ != "localhost")
  }

  override def checkpoint() {
    // Do nothing. Hadoop RDD should not be checkpointed.
  }

  def getConf: Configuration = confBroadcast.value.value
}
