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

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import org.apache.spark.executor.{ShuffleReadMetrics, TaskMetrics}
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.CompletionIterator


private[spark] class BlockStoreShuffleFetcher extends ShuffleFetcher with Logging {

  override def fetch[T](shuffleId: Int, reduceId: Int, metrics: TaskMetrics, serializer: Serializer)
    : Iterator[T] =
  {

    logDebug("Fetching outputs for shuffle %d, reduce %d".format(shuffleId, reduceId))
    val blockManager = SparkEnv.get.blockManager

    val startTime = System.currentTimeMillis
    val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(shuffleId, reduceId)
    logDebug("Fetching map output location for shuffle %d, reduce %d took %d ms".format(
      shuffleId, reduceId, System.currentTimeMillis - startTime))

    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[(Int, Long)]]
    for (((address, size), index) <- statuses.zipWithIndex) {
      splitsByAddress.getOrElseUpdate(address, ArrayBuffer()) += ((index, size))
    }

    val blocksByAddress: Seq[(BlockManagerId, Seq[(String, Long)])] = splitsByAddress.toSeq.map {
      case (address, splits) =>
        (address, splits.map(s => ("shuffle_%d_%d_%d".format(shuffleId, s._1, reduceId), s._2)))
    }

    def unpackBlock(blockPair: (String, Option[Iterator[Any]])) : Iterator[T] = {
      val blockId = blockPair._1
      val blockOption = blockPair._2
      blockOption match {
        case Some(block) => {
          block.asInstanceOf[Iterator[T]]
        }
        case None => {
          val regex = "shuffle_([0-9]*)_([0-9]*)_([0-9]*)".r
          blockId match {
            case regex(shufId, mapId, _) =>
              val address = statuses(mapId.toInt)._1
              throw new FetchFailedException(address, shufId.toInt, mapId.toInt, reduceId, null)
            case _ =>
              throw new SparkException(
                "Failed to get block " + blockId + ", which is not a shuffle block")
          }
        }
      }
    }

    val blockFetcherItr = blockManager.getMultiple(blocksByAddress, serializer)
    val itr = blockFetcherItr.flatMap(unpackBlock)

    CompletionIterator[T, Iterator[T]](itr, {
      val shuffleMetrics = new ShuffleReadMetrics
      shuffleMetrics.shuffleFinishTime = System.currentTimeMillis
      shuffleMetrics.remoteFetchTime = blockFetcherItr.remoteFetchTime
      shuffleMetrics.fetchWaitTime = blockFetcherItr.fetchWaitTime
      shuffleMetrics.remoteBytesRead = blockFetcherItr.remoteBytesRead
      shuffleMetrics.totalBlocksFetched = blockFetcherItr.totalBlocks
      shuffleMetrics.localBlocksFetched = blockFetcherItr.numLocalBlocks
      shuffleMetrics.remoteBlocksFetched = blockFetcherItr.numRemoteBlocks
      metrics.shuffleReadMetrics = Some(shuffleMetrics)
    })
  }
}
