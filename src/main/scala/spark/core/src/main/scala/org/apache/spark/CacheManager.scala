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

import org.apache.spark.executor.InputMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.storage._

/**
 * Spark class responsible for passing RDDs partition contents to the BlockManager and making
 * sure a node doesn't load two copies of an RDD at once.
 */
private[spark] class CacheManager(blockManager: BlockManager) extends Logging {

  /** Keys of RDD partitions that are being computed/loaded. */
  private val loading = new HashSet[RDDBlockId]()

  /** Gets or computes an RDD partition. Used by RDD.iterator() when an RDD is cached. */
  def getOrCompute[T](
      rdd: RDD[T],
      partition: Partition,
      context: TaskContext,
      storageLevel: StorageLevel): Iterator[T] = {

    val key = RDDBlockId(rdd.id, partition.index)
    logDebug(s"Looking for partition $key")
    blockManager.get(key) match {
      case Some(blockResult) =>
        // Partition is already materialized, so just return its values
        context.taskMetrics.inputMetrics = Some(blockResult.inputMetrics)
        new InterruptibleIterator(context, blockResult.data.asInstanceOf[Iterator[T]])

      case None =>
        // Acquire a lock for loading this partition
        // If another thread already holds the lock, wait for it to finish return its results
        val storedValues = acquireLockForPartition[T](key)
        if (storedValues.isDefined) {
          return new InterruptibleIterator[T](context, storedValues.get)
        }

        // Otherwise, we have to load the partition ourselves
        try {
          logInfo(s"Partition $key not found, computing it")
          val computedValues = rdd.computeOrReadCheckpoint(partition, context)

          // If the task is running locally, do not persist the result
          if (context.runningLocally) {
            return computedValues
          }

          // Otherwise, cache the values and keep track of any updates in block statuses
          val updatedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
          val cachedValues = putInBlockManager(key, computedValues, storageLevel, updatedBlocks)
          context.taskMetrics.updatedBlocks = Some(updatedBlocks)
          new InterruptibleIterator(context, cachedValues)

        } finally {
          loading.synchronized {
            loading.remove(key)
            loading.notifyAll()
          }
        }
    }
  }

  /**
   * Acquire a loading lock for the partition identified by the given block ID.
   *
   * If the lock is free, just acquire it and return None. Otherwise, another thread is already
   * loading the partition, so we wait for it to finish and return the values loaded by the thread.
   */
  private def acquireLockForPartition[T](id: RDDBlockId): Option[Iterator[T]] = {
    loading.synchronized {
      if (!loading.contains(id)) {
        // If the partition is free, acquire its lock to compute its value
        loading.add(id)
        None
      } else {
        // Otherwise, wait for another thread to finish and return its result
        logInfo(s"Another thread is loading $id, waiting for it to finish...")
        while (loading.contains(id)) {
          try {
            loading.wait()
          } catch {
            case e: Exception =>
              logWarning(s"Exception while waiting for another thread to load $id", e)
          }
        }
        logInfo(s"Finished waiting for $id")
        val values = blockManager.get(id)
        if (!values.isDefined) {
          /* The block is not guaranteed to exist even after the other thread has finished.
           * For instance, the block could be evicted after it was put, but before our get.
           * In this case, we still need to load the partition ourselves. */
          logInfo(s"Whoever was loading $id failed; we'll try it ourselves")
          loading.add(id)
        }
        values.map(_.data.asInstanceOf[Iterator[T]])
      }
    }
  }

  /**
   * Cache the values of a partition, keeping track of any updates in the storage statuses
   * of other blocks along the way.
   */
  private def putInBlockManager[T](
      key: BlockId,
      values: Iterator[T],
      storageLevel: StorageLevel,
      updatedBlocks: ArrayBuffer[(BlockId, BlockStatus)]): Iterator[T] = {

    if (!storageLevel.useMemory) {
      /* This RDD is not to be cached in memory, so we can just pass the computed values
       * as an iterator directly to the BlockManager, rather than first fully unrolling
       * it in memory. The latter option potentially uses much more memory and risks OOM
       * exceptions that can be avoided. */
      updatedBlocks ++= blockManager.put(key, values, storageLevel, tellMaster = true)
      blockManager.get(key) match {
        case Some(v) => v.data.asInstanceOf[Iterator[T]]
        case None =>
          logInfo(s"Failure to store $key")
          throw new BlockException(key, s"Block manager failed to return cached value for $key!")
      }
    } else {
      /* This RDD is to be cached in memory. In this case we cannot pass the computed values
       * to the BlockManager as an iterator and expect to read it back later. This is because
       * we may end up dropping a partition from memory store before getting it back, e.g.
       * when the entirety of the RDD does not fit in memory. */
      val elements = new ArrayBuffer[Any]
      elements ++= values
      updatedBlocks ++= blockManager.put(key, elements, storageLevel, tellMaster = true)
      elements.iterator.asInstanceOf[Iterator[T]]
    }
  }

}
