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

package org.apache.spark

import scala.collection.mutable.{ArrayBuffer, HashSet}
import org.apache.spark.storage.{BlockManager, StorageLevel}
import org.apache.spark.rdd.RDD


/** Spark class responsible for passing RDDs split contents to the BlockManager and making
    sure a node doesn't load two copies of an RDD at once.
  */
private[spark] class CacheManager(blockManager: BlockManager) extends Logging {
  private val loading = new HashSet[String]

  /** Gets or computes an RDD split. Used by RDD.iterator() when an RDD is cached. */
  def getOrCompute[T](rdd: RDD[T], split: Partition, context: TaskContext, storageLevel: StorageLevel)
      : Iterator[T] = {
    val key = "rdd_%d_%d".format(rdd.id, split.index)
    logInfo("Cache key is " + key)
    blockManager.get(key) match {
      case Some(cachedValues) =>
        // Partition is in cache, so just return its values
        logInfo("Found partition in cache!")
        return cachedValues.asInstanceOf[Iterator[T]]

      case None =>
        // Mark the split as loading (unless someone else marks it first)
        loading.synchronized {
          if (loading.contains(key)) {
            logInfo("Loading contains " + key + ", waiting...")
            while (loading.contains(key)) {
              try {loading.wait()} catch {case _ : Throwable =>}
            }
            logInfo("Loading no longer contains " + key + ", so returning cached result")
            // See whether someone else has successfully loaded it. The main way this would fail
            // is for the RDD-level cache eviction policy if someone else has loaded the same RDD
            // partition but we didn't want to make space for it. However, that case is unlikely
            // because it's unlikely that two threads would work on the same RDD partition. One
            // downside of the current code is that threads wait serially if this does happen.
            blockManager.get(key) match {
              case Some(values) =>
                return values.asInstanceOf[Iterator[T]]
              case None =>
                logInfo("Whoever was loading " + key + " failed; we'll try it ourselves")
                loading.add(key)
            }
          } else {
            loading.add(key)
          }
        }
        try {
          // If we got here, we have to load the split
          logInfo("Computing partition " + split)
          val computedValues = rdd.computeOrReadCheckpoint(split, context)
          // Persist the result, so long as the task is not running locally
          if (context.runningLocally) { return computedValues }
          val elements = new ArrayBuffer[Any]
          elements ++= computedValues
          blockManager.put(key, elements, storageLevel, true)
          return elements.iterator.asInstanceOf[Iterator[T]]
        } finally {
          loading.synchronized {
            loading.remove(key)
            loading.notifyAll()
          }
        }
    }
  }
}
