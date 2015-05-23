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

import org.scalatest.FunSuite

import org.apache.spark.storage._
import org.apache.spark.broadcast.{Broadcast, HttpBroadcast}
import org.apache.spark.storage.BroadcastBlockId

class BroadcastSuite extends FunSuite with LocalSparkContext {

  private val httpConf = broadcastConf("HttpBroadcastFactory")
  private val torrentConf = broadcastConf("TorrentBroadcastFactory")

  test("Using HttpBroadcast locally") {
    sc = new SparkContext("local", "test", httpConf)
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to 2).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === Set((1, 10), (2, 10)))
  }

  test("Accessing HttpBroadcast variables from multiple threads") {
    sc = new SparkContext("local[10]", "test", httpConf)
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to 10).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === (1 to 10).map(x => (x, 10)).toSet)
  }

  test("Accessing HttpBroadcast variables in a local cluster") {
    val numSlaves = 4
    sc = new SparkContext("local-cluster[%d, 1, 512]".format(numSlaves), "test", httpConf)
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to numSlaves).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === (1 to numSlaves).map(x => (x, 10)).toSet)
  }

  test("Using TorrentBroadcast locally") {
    sc = new SparkContext("local", "test", torrentConf)
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to 2).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === Set((1, 10), (2, 10)))
  }

  test("Accessing TorrentBroadcast variables from multiple threads") {
    sc = new SparkContext("local[10]", "test", torrentConf)
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to 10).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === (1 to 10).map(x => (x, 10)).toSet)
  }

  test("Accessing TorrentBroadcast variables in a local cluster") {
    val numSlaves = 4
    sc = new SparkContext("local-cluster[%d, 1, 512]".format(numSlaves), "test", torrentConf)
    val list = List[Int](1, 2, 3, 4)
    val broadcast = sc.broadcast(list)
    val results = sc.parallelize(1 to numSlaves).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === (1 to numSlaves).map(x => (x, 10)).toSet)
  }

  test("Unpersisting HttpBroadcast on executors only in local mode") {
    testUnpersistHttpBroadcast(distributed = false, removeFromDriver = false)
  }

  test("Unpersisting HttpBroadcast on executors and driver in local mode") {
    testUnpersistHttpBroadcast(distributed = false, removeFromDriver = true)
  }

  test("Unpersisting HttpBroadcast on executors only in distributed mode") {
    testUnpersistHttpBroadcast(distributed = true, removeFromDriver = false)
  }

  test("Unpersisting HttpBroadcast on executors and driver in distributed mode") {
    testUnpersistHttpBroadcast(distributed = true, removeFromDriver = true)
  }

  test("Unpersisting TorrentBroadcast on executors only in local mode") {
    testUnpersistTorrentBroadcast(distributed = false, removeFromDriver = false)
  }

  test("Unpersisting TorrentBroadcast on executors and driver in local mode") {
    testUnpersistTorrentBroadcast(distributed = false, removeFromDriver = true)
  }

  test("Unpersisting TorrentBroadcast on executors only in distributed mode") {
    testUnpersistTorrentBroadcast(distributed = true, removeFromDriver = false)
  }

  test("Unpersisting TorrentBroadcast on executors and driver in distributed mode") {
    testUnpersistTorrentBroadcast(distributed = true, removeFromDriver = true)
  }
  /**
   * Verify the persistence of state associated with an HttpBroadcast in either local mode or
   * local-cluster mode (when distributed = true).
   *
   * This test creates a broadcast variable, uses it on all executors, and then unpersists it.
   * In between each step, this test verifies that the broadcast blocks and the broadcast file
   * are present only on the expected nodes.
   */
  private def testUnpersistHttpBroadcast(distributed: Boolean, removeFromDriver: Boolean) {
    val numSlaves = if (distributed) 2 else 0

    def getBlockIds(id: Long) = Seq[BroadcastBlockId](BroadcastBlockId(id))

    // Verify that the broadcast file is created, and blocks are persisted only on the driver
    def afterCreation(blockIds: Seq[BroadcastBlockId], bmm: BlockManagerMaster) {
      assert(blockIds.size === 1)
      val statuses = bmm.getBlockStatus(blockIds.head, askSlaves = true)
      assert(statuses.size === 1)
      statuses.head match { case (bm, status) =>
        assert(bm.executorId === "<driver>", "Block should only be on the driver")
        assert(status.storageLevel === StorageLevel.MEMORY_AND_DISK)
        assert(status.memSize > 0, "Block should be in memory store on the driver")
        assert(status.diskSize === 0, "Block should not be in disk store on the driver")
      }
      if (distributed) {
        // this file is only generated in distributed mode
        assert(HttpBroadcast.getFile(blockIds.head.broadcastId).exists, "Broadcast file not found!")
      }
    }

    // Verify that blocks are persisted in both the executors and the driver
    def afterUsingBroadcast(blockIds: Seq[BroadcastBlockId], bmm: BlockManagerMaster) {
      assert(blockIds.size === 1)
      val statuses = bmm.getBlockStatus(blockIds.head, askSlaves = true)
      assert(statuses.size === numSlaves + 1)
      statuses.foreach { case (_, status) =>
        assert(status.storageLevel === StorageLevel.MEMORY_AND_DISK)
        assert(status.memSize > 0, "Block should be in memory store")
        assert(status.diskSize === 0, "Block should not be in disk store")
      }
    }

    // Verify that blocks are unpersisted on all executors, and on all nodes if removeFromDriver
    // is true. In the latter case, also verify that the broadcast file is deleted on the driver.
    def afterUnpersist(blockIds: Seq[BroadcastBlockId], bmm: BlockManagerMaster) {
      assert(blockIds.size === 1)
      val statuses = bmm.getBlockStatus(blockIds.head, askSlaves = true)
      val expectedNumBlocks = if (removeFromDriver) 0 else 1
      val possiblyNot = if (removeFromDriver) "" else " not"
      assert(statuses.size === expectedNumBlocks,
        "Block should%s be unpersisted on the driver".format(possiblyNot))
      if (distributed && removeFromDriver) {
        // this file is only generated in distributed mode
        assert(!HttpBroadcast.getFile(blockIds.head.broadcastId).exists,
          "Broadcast file should%s be deleted".format(possiblyNot))
      }
    }

    testUnpersistBroadcast(distributed, numSlaves, httpConf, getBlockIds, afterCreation,
      afterUsingBroadcast, afterUnpersist, removeFromDriver)
  }

  /**
   * Verify the persistence of state associated with an TorrentBroadcast in a local-cluster.
   *
   * This test creates a broadcast variable, uses it on all executors, and then unpersists it.
   * In between each step, this test verifies that the broadcast blocks are present only on the
   * expected nodes.
   */
  private def testUnpersistTorrentBroadcast(distributed: Boolean, removeFromDriver: Boolean) {
    val numSlaves = if (distributed) 2 else 0

    def getBlockIds(id: Long) = {
      val broadcastBlockId = BroadcastBlockId(id)
      val metaBlockId = BroadcastBlockId(id, "meta")
      // Assume broadcast value is small enough to fit into 1 piece
      val pieceBlockId = BroadcastBlockId(id, "piece0")
      if (distributed) {
        // the metadata and piece blocks are generated only in distributed mode
        Seq[BroadcastBlockId](broadcastBlockId, metaBlockId, pieceBlockId)
      } else {
        Seq[BroadcastBlockId](broadcastBlockId)
      }
    }

    // Verify that blocks are persisted only on the driver
    def afterCreation(blockIds: Seq[BroadcastBlockId], bmm: BlockManagerMaster) {
      blockIds.foreach { blockId =>
        val statuses = bmm.getBlockStatus(blockIds.head, askSlaves = true)
        assert(statuses.size === 1)
        statuses.head match { case (bm, status) =>
          assert(bm.executorId === "<driver>", "Block should only be on the driver")
          assert(status.storageLevel === StorageLevel.MEMORY_AND_DISK)
          assert(status.memSize > 0, "Block should be in memory store on the driver")
          assert(status.diskSize === 0, "Block should not be in disk store on the driver")
        }
      }
    }

    // Verify that blocks are persisted in both the executors and the driver
    def afterUsingBroadcast(blockIds: Seq[BroadcastBlockId], bmm: BlockManagerMaster) {
      blockIds.foreach { blockId =>
        val statuses = bmm.getBlockStatus(blockId, askSlaves = true)
        if (blockId.field == "meta") {
          // Meta data is only on the driver
          assert(statuses.size === 1)
          statuses.head match { case (bm, _) => assert(bm.executorId === "<driver>") }
        } else {
          // Other blocks are on both the executors and the driver
          assert(statuses.size === numSlaves + 1,
            blockId + " has " + statuses.size + " statuses: " + statuses.mkString(","))
          statuses.foreach { case (_, status) =>
            assert(status.storageLevel === StorageLevel.MEMORY_AND_DISK)
            assert(status.memSize > 0, "Block should be in memory store")
            assert(status.diskSize === 0, "Block should not be in disk store")
          }
        }
      }
    }

    // Verify that blocks are unpersisted on all executors, and on all nodes if removeFromDriver
    // is true.
    def afterUnpersist(blockIds: Seq[BroadcastBlockId], bmm: BlockManagerMaster) {
      val expectedNumBlocks = if (removeFromDriver) 0 else 1
      val possiblyNot = if (removeFromDriver) "" else " not"
      blockIds.foreach { blockId =>
        val statuses = bmm.getBlockStatus(blockId, askSlaves = true)
        assert(statuses.size === expectedNumBlocks,
          "Block should%s be unpersisted on the driver".format(possiblyNot))
      }
    }

    testUnpersistBroadcast(distributed, numSlaves,  torrentConf, getBlockIds, afterCreation,
      afterUsingBroadcast, afterUnpersist, removeFromDriver)
  }

  /**
   * This test runs in 4 steps:
   *
   * 1) Create broadcast variable, and verify that all state is persisted on the driver.
   * 2) Use the broadcast variable on all executors, and verify that all state is persisted
   *    on both the driver and the executors.
   * 3) Unpersist the broadcast, and verify that all state is removed where they should be.
   * 4) [Optional] If removeFromDriver is false, we verify that the broadcast is re-usable.
   */
  private def testUnpersistBroadcast(
      distributed: Boolean,
      numSlaves: Int,  // used only when distributed = true
      broadcastConf: SparkConf,
      getBlockIds: Long => Seq[BroadcastBlockId],
      afterCreation: (Seq[BroadcastBlockId], BlockManagerMaster) => Unit,
      afterUsingBroadcast: (Seq[BroadcastBlockId], BlockManagerMaster) => Unit,
      afterUnpersist: (Seq[BroadcastBlockId], BlockManagerMaster) => Unit,
      removeFromDriver: Boolean) {

    sc = if (distributed) {
      new SparkContext("local-cluster[%d, 1, 512]".format(numSlaves), "test", broadcastConf)
    } else {
      new SparkContext("local", "test", broadcastConf)
    }
    val blockManagerMaster = sc.env.blockManager.master
    val list = List[Int](1, 2, 3, 4)

    // Create broadcast variable
    val broadcast = sc.broadcast(list)
    val blocks = getBlockIds(broadcast.id)
    afterCreation(blocks, blockManagerMaster)

    // Use broadcast variable on all executors
    val partitions = 10
    assert(partitions > numSlaves)
    val results = sc.parallelize(1 to partitions, partitions).map(x => (x, broadcast.value.sum))
    assert(results.collect().toSet === (1 to partitions).map(x => (x, list.sum)).toSet)
    afterUsingBroadcast(blocks, blockManagerMaster)

    // Unpersist broadcast
    if (removeFromDriver) {
      broadcast.destroy(blocking = true)
    } else {
      broadcast.unpersist(blocking = true)
    }
    afterUnpersist(blocks, blockManagerMaster)

    // If the broadcast is removed from driver, all subsequent uses of the broadcast variable
    // should throw SparkExceptions. Otherwise, the result should be the same as before.
    if (removeFromDriver) {
      // Using this variable on the executors crashes them, which hangs the test.
      // Instead, crash the driver by directly accessing the broadcast value.
      intercept[SparkException] { broadcast.value }
      intercept[SparkException] { broadcast.unpersist() }
      intercept[SparkException] { broadcast.destroy(blocking = true) }
    } else {
      val results = sc.parallelize(1 to partitions, partitions).map(x => (x, broadcast.value.sum))
      assert(results.collect().toSet === (1 to partitions).map(x => (x, list.sum)).toSet)
    }
  }

  /** Helper method to create a SparkConf that uses the given broadcast factory. */
  private def broadcastConf(factoryName: String): SparkConf = {
    val conf = new SparkConf
    conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.%s".format(factoryName))
    conf
  }
}
