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

package org.apache.spark.scheduler

import java.util.Random

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

import org.scalatest.FunSuite

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.util.FakeClock

class FakeDAGScheduler(sc: SparkContext, taskScheduler: FakeTaskScheduler)
  extends DAGScheduler(sc) {

  override def taskStarted(task: Task[_], taskInfo: TaskInfo) {
    taskScheduler.startedTasks += taskInfo.index
  }

  override def taskEnded(
      task: Task[_],
      reason: TaskEndReason,
      result: Any,
      accumUpdates: mutable.Map[Long, Any],
      taskInfo: TaskInfo,
      taskMetrics: TaskMetrics) {
    taskScheduler.endedTasks(taskInfo.index) = reason
  }

  override def executorAdded(execId: String, host: String) {}

  override def executorLost(execId: String) {}

  override def taskSetFailed(taskSet: TaskSet, reason: String) {
    taskScheduler.taskSetsFailed += taskSet.id
  }
}

/**
 * A mock TaskSchedulerImpl implementation that just remembers information about tasks started and
 * feedback received from the TaskSetManagers. Note that it's important to initialize this with
 * a list of "live" executors and their hostnames for isExecutorAlive and hasExecutorsAliveOnHost
 * to work, and these are required for locality in TaskSetManager.
 */
class FakeTaskScheduler(sc: SparkContext, liveExecutors: (String, String)* /* execId, host */)
  extends TaskSchedulerImpl(sc)
{
  val startedTasks = new ArrayBuffer[Long]
  val endedTasks = new mutable.HashMap[Long, TaskEndReason]
  val finishedManagers = new ArrayBuffer[TaskSetManager]
  val taskSetsFailed = new ArrayBuffer[String]

  val executors = new mutable.HashMap[String, String] ++ liveExecutors

  dagScheduler = new FakeDAGScheduler(sc, this)

  def removeExecutor(execId: String): Unit = executors -= execId

  override def taskSetFinished(manager: TaskSetManager): Unit = finishedManagers += manager

  override def isExecutorAlive(execId: String): Boolean = executors.contains(execId)

  override def hasExecutorsAliveOnHost(host: String): Boolean = executors.values.exists(_ == host)

  def addExecutor(execId: String, host: String) {
    executors.put(execId, host)
  }
}

/**
 * A Task implementation that results in a large serialized task.
 */
class LargeTask(stageId: Int) extends Task[Array[Byte]](stageId, 0) {
  val randomBuffer = new Array[Byte](TaskSetManager.TASK_SIZE_TO_WARN_KB * 1024)
  val random = new Random(0)
  random.nextBytes(randomBuffer)

  override def runTask(context: TaskContext): Array[Byte] = randomBuffer
  override def preferredLocations: Seq[TaskLocation] = Seq[TaskLocation]()
}

class TaskSetManagerSuite extends FunSuite with LocalSparkContext with Logging {
  import TaskLocality.{ANY, PROCESS_LOCAL, NODE_LOCAL, RACK_LOCAL}

  private val conf = new SparkConf

  val LOCALITY_WAIT = conf.getLong("spark.locality.wait", 3000)
  val MAX_TASK_FAILURES = 4

  test("TaskSet with no preferences") {
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val taskSet = FakeTask.createTaskSet(1)
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES)

    // Offer a host with process-local as the constraint; this should work because the TaskSet
    // above won't have any locality preferences
    val taskOption = manager.resourceOffer("exec1", "host1", TaskLocality.PROCESS_LOCAL)
    assert(taskOption.isDefined)
    val task = taskOption.get
    assert(task.executorId === "exec1")
    assert(sched.startedTasks.contains(0))

    // Re-offer the host -- now we should get no more tasks
    assert(manager.resourceOffer("exec1", "host1", PROCESS_LOCAL) === None)

    // Tell it the task has finished
    manager.handleSuccessfulTask(0, createTaskResult(0))
    assert(sched.endedTasks(0) === Success)
    assert(sched.finishedManagers.contains(manager))
  }

  test("multiple offers with no preferences") {
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val taskSet = FakeTask.createTaskSet(3)
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES)

    // First three offers should all find tasks
    for (i <- 0 until 3) {
      val taskOption = manager.resourceOffer("exec1", "host1", PROCESS_LOCAL)
      assert(taskOption.isDefined)
      val task = taskOption.get
      assert(task.executorId === "exec1")
    }
    assert(sched.startedTasks.toSet === Set(0, 1, 2))

    // Re-offer the host -- now we should get no more tasks
    assert(manager.resourceOffer("exec1", "host1", PROCESS_LOCAL) === None)

    // Finish the first two tasks
    manager.handleSuccessfulTask(0, createTaskResult(0))
    manager.handleSuccessfulTask(1, createTaskResult(1))
    assert(sched.endedTasks(0) === Success)
    assert(sched.endedTasks(1) === Success)
    assert(!sched.finishedManagers.contains(manager))

    // Finish the last task
    manager.handleSuccessfulTask(2, createTaskResult(2))
    assert(sched.endedTasks(2) === Success)
    assert(sched.finishedManagers.contains(manager))
  }

  test("skip unsatisfiable locality levels") {
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc, ("execA", "host1"), ("execC", "host2"))
    val taskSet = FakeTask.createTaskSet(1, Seq(TaskLocation("host1", "execB")))
    val clock = new FakeClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    // An executor that is not NODE_LOCAL should be rejected.
    assert(manager.resourceOffer("execC", "host2", ANY) === None)

    // Because there are no alive PROCESS_LOCAL executors, the base locality level should be
    // NODE_LOCAL. So, we should schedule the task on this offered NODE_LOCAL executor before
    // any of the locality wait timers expire.
    assert(manager.resourceOffer("execA", "host1", ANY).get.index === 0)
  }

  test("basic delay scheduling") {
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc, ("exec1", "host1"), ("exec2", "host2"))
    val taskSet = FakeTask.createTaskSet(4,
      Seq(TaskLocation("host1", "exec1")),
      Seq(TaskLocation("host2", "exec2")),
      Seq(TaskLocation("host1"), TaskLocation("host2", "exec2")),
      Seq()   // Last task has no locality prefs
    )
    val clock = new FakeClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    // First offer host1, exec1: first task should be chosen
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 0)

    // Offer host1, exec1 again: the last task, which has no prefs, should be chosen
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 3)

    // Offer host1, exec1 again, at PROCESS_LOCAL level: nothing should get chosen
    assert(manager.resourceOffer("exec1", "host1", PROCESS_LOCAL) === None)

    clock.advance(LOCALITY_WAIT)

    // Offer host1, exec1 again, at PROCESS_LOCAL level: nothing should get chosen
    assert(manager.resourceOffer("exec1", "host1", PROCESS_LOCAL) === None)

    // Offer host1, exec1 again, at NODE_LOCAL level: we should choose task 2
    assert(manager.resourceOffer("exec1", "host1", NODE_LOCAL).get.index == 2)

    // Offer host1, exec1 again, at NODE_LOCAL level: nothing should get chosen
    assert(manager.resourceOffer("exec1", "host1", NODE_LOCAL) === None)

    // Offer host1, exec1 again, at ANY level: nothing should get chosen
    assert(manager.resourceOffer("exec1", "host1", ANY) === None)

    clock.advance(LOCALITY_WAIT)

    // Offer host1, exec1 again, at ANY level: task 1 should get chosen
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 1)

    // Offer host1, exec1 again, at ANY level: nothing should be chosen as we've launched all tasks
    assert(manager.resourceOffer("exec1", "host1", ANY) === None)
  }

  test("delay scheduling with fallback") {
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc,
      ("exec1", "host1"), ("exec2", "host2"), ("exec3", "host3"))
    val taskSet = FakeTask.createTaskSet(5,
      Seq(TaskLocation("host1")),
      Seq(TaskLocation("host2")),
      Seq(TaskLocation("host2")),
      Seq(TaskLocation("host3")),
      Seq(TaskLocation("host2"))
    )
    val clock = new FakeClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    // First offer host1: first task should be chosen
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 0)

    // Offer host1 again: nothing should get chosen
    assert(manager.resourceOffer("exec1", "host1", ANY) === None)

    clock.advance(LOCALITY_WAIT)

    // Offer host1 again: second task (on host2) should get chosen
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 1)

    // Offer host1 again: third task (on host2) should get chosen
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 2)

    // Offer host2: fifth task (also on host2) should get chosen
    assert(manager.resourceOffer("exec2", "host2", ANY).get.index === 4)

    // Now that we've launched a local task, we should no longer launch the task for host3
    assert(manager.resourceOffer("exec2", "host2", ANY) === None)

    clock.advance(LOCALITY_WAIT)

    // After another delay, we can go ahead and launch that task non-locally
    assert(manager.resourceOffer("exec2", "host2", ANY).get.index === 3)
  }

  test("delay scheduling with failed hosts") {
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc, ("exec1", "host1"), ("exec2", "host2"))
    val taskSet = FakeTask.createTaskSet(3,
      Seq(TaskLocation("host1")),
      Seq(TaskLocation("host2")),
      Seq(TaskLocation("host3"))
    )
    val clock = new FakeClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    // First offer host1: first task should be chosen
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 0)

    // Offer host1 again: third task should be chosen immediately because host3 is not up
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 2)

    // After this, nothing should get chosen
    assert(manager.resourceOffer("exec1", "host1", ANY) === None)

    // Now mark host2 as dead
    sched.removeExecutor("exec2")
    manager.executorLost("exec2", "host2")

    // Task 1 should immediately be launched on host1 because its original host is gone
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 1)

    // Now that all tasks have launched, nothing new should be launched anywhere else
    assert(manager.resourceOffer("exec1", "host1", ANY) === None)
    assert(manager.resourceOffer("exec2", "host2", ANY) === None)
  }

  test("task result lost") {
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val taskSet = FakeTask.createTaskSet(1)
    val clock = new FakeClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 0)

    // Tell it the task has finished but the result was lost.
    manager.handleFailedTask(0, TaskState.FINISHED, TaskResultLost)
    assert(sched.endedTasks(0) === TaskResultLost)

    // Re-offer the host -- now we should get task 0 again.
    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 0)
  }

  test("repeated failures lead to task set abortion") {
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val taskSet = FakeTask.createTaskSet(1)
    val clock = new FakeClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)

    // Fail the task MAX_TASK_FAILURES times, and check that the task set is aborted
    // after the last failure.
    (1 to manager.maxTaskFailures).foreach { index =>
      val offerResult = manager.resourceOffer("exec1", "host1", ANY)
      assert(offerResult.isDefined,
        "Expect resource offer on iteration %s to return a task".format(index))
      assert(offerResult.get.index === 0)
      manager.handleFailedTask(offerResult.get.taskId, TaskState.FINISHED, TaskResultLost)
      if (index < MAX_TASK_FAILURES) {
        assert(!sched.taskSetsFailed.contains(taskSet.id))
      } else {
        assert(sched.taskSetsFailed.contains(taskSet.id))
      }
    }
  }

  test("executors should be blacklisted after task failure, in spite of locality preferences") {
    val rescheduleDelay = 300L
    val conf = new SparkConf().
      set("spark.scheduler.executorTaskBlacklistTime", rescheduleDelay.toString).
      // dont wait to jump locality levels in this test
      set("spark.locality.wait", "0")

    sc = new SparkContext("local", "test", conf)
    // two executors on same host, one on different.
    val sched = new FakeTaskScheduler(sc, ("exec1", "host1"),
      ("exec1.1", "host1"), ("exec2", "host2"))
    // affinity to exec1 on host1 - which we will fail.
    val taskSet = FakeTask.createTaskSet(1, Seq(TaskLocation("host1", "exec1")))
    val clock = new FakeClock
    val manager = new TaskSetManager(sched, taskSet, 4, clock)

    {
      val offerResult = manager.resourceOffer("exec1", "host1", TaskLocality.PROCESS_LOCAL)
      assert(offerResult.isDefined, "Expect resource offer to return a task")

      assert(offerResult.get.index === 0)
      assert(offerResult.get.executorId === "exec1")

      // Cause exec1 to fail : failure 1
      manager.handleFailedTask(offerResult.get.taskId, TaskState.FINISHED, TaskResultLost)
      assert(!sched.taskSetsFailed.contains(taskSet.id))

      // Ensure scheduling on exec1 fails after failure 1 due to blacklist
      assert(manager.resourceOffer("exec1", "host1", TaskLocality.PROCESS_LOCAL).isEmpty)
      assert(manager.resourceOffer("exec1", "host1", TaskLocality.NODE_LOCAL).isEmpty)
      assert(manager.resourceOffer("exec1", "host1", TaskLocality.RACK_LOCAL).isEmpty)
      assert(manager.resourceOffer("exec1", "host1", TaskLocality.ANY).isEmpty)
    }

    // Run the task on exec1.1 - should work, and then fail it on exec1.1
    {
      val offerResult = manager.resourceOffer("exec1.1", "host1", TaskLocality.NODE_LOCAL)
      assert(offerResult.isDefined,
        "Expect resource offer to return a task for exec1.1, offerResult = " + offerResult)

      assert(offerResult.get.index === 0)
      assert(offerResult.get.executorId === "exec1.1")

      // Cause exec1.1 to fail : failure 2
      manager.handleFailedTask(offerResult.get.taskId, TaskState.FINISHED, TaskResultLost)
      assert(!sched.taskSetsFailed.contains(taskSet.id))

      // Ensure scheduling on exec1.1 fails after failure 2 due to blacklist
      assert(manager.resourceOffer("exec1.1", "host1", TaskLocality.NODE_LOCAL).isEmpty)
    }

    // Run the task on exec2 - should work, and then fail it on exec2
    {
      val offerResult = manager.resourceOffer("exec2", "host2", TaskLocality.ANY)
      assert(offerResult.isDefined, "Expect resource offer to return a task")

      assert(offerResult.get.index === 0)
      assert(offerResult.get.executorId === "exec2")

      // Cause exec2 to fail : failure 3
      manager.handleFailedTask(offerResult.get.taskId, TaskState.FINISHED, TaskResultLost)
      assert(!sched.taskSetsFailed.contains(taskSet.id))

      // Ensure scheduling on exec2 fails after failure 3 due to blacklist
      assert(manager.resourceOffer("exec2", "host2", TaskLocality.ANY).isEmpty)
    }

    // After reschedule delay, scheduling on exec1 should be possible.
    clock.advance(rescheduleDelay)

    {
      val offerResult = manager.resourceOffer("exec1", "host1", TaskLocality.PROCESS_LOCAL)
      assert(offerResult.isDefined, "Expect resource offer to return a task")

      assert(offerResult.get.index === 0)
      assert(offerResult.get.executorId === "exec1")

      assert(manager.resourceOffer("exec1", "host1", TaskLocality.PROCESS_LOCAL).isEmpty)

      // Cause exec1 to fail : failure 4
      manager.handleFailedTask(offerResult.get.taskId, TaskState.FINISHED, TaskResultLost)
    }

    // we have failed the same task 4 times now : task id should now be in taskSetsFailed
    assert(sched.taskSetsFailed.contains(taskSet.id))
  }

  test("new executors get added") {
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc)
    val taskSet = FakeTask.createTaskSet(4,
      Seq(TaskLocation("host1", "execA")),
      Seq(TaskLocation("host1", "execB")),
      Seq(TaskLocation("host2", "execC")),
      Seq())
    val clock = new FakeClock
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES, clock)
    // All tasks added to no-pref list since no preferred location is available
    assert(manager.pendingTasksWithNoPrefs.size === 4)
    // Only ANY is valid
    assert(manager.myLocalityLevels.sameElements(Array(ANY)))
    // Add a new executor
    sched.addExecutor("execD", "host1")
    manager.executorAdded()
    // Task 0 and 1 should be removed from no-pref list
    assert(manager.pendingTasksWithNoPrefs.size === 2)
    // Valid locality should contain NODE_LOCAL and ANY
    assert(manager.myLocalityLevels.sameElements(Array(NODE_LOCAL, ANY)))
    // Add another executor
    sched.addExecutor("execC", "host2")
    manager.executorAdded()
    // No-pref list now only contains task 3
    assert(manager.pendingTasksWithNoPrefs.size === 1)
    // Valid locality should contain PROCESS_LOCAL, NODE_LOCAL and ANY
    assert(manager.myLocalityLevels.sameElements(Array(PROCESS_LOCAL, NODE_LOCAL, ANY)))
  }

  test("do not emit warning when serialized task is small") {
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc, ("exec1", "host1"))
    val taskSet = FakeTask.createTaskSet(1)
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES)

    assert(!manager.emittedTaskSizeWarning)

    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 0)

    assert(!manager.emittedTaskSizeWarning)
  }

  test("emit warning when serialized task is large") {
    sc = new SparkContext("local", "test")
    val sched = new FakeTaskScheduler(sc, ("exec1", "host1"))

    val taskSet = new TaskSet(Array(new LargeTask(0)), 0, 0, 0, null)
    val manager = new TaskSetManager(sched, taskSet, MAX_TASK_FAILURES)

    assert(!manager.emittedTaskSizeWarning)

    assert(manager.resourceOffer("exec1", "host1", ANY).get.index === 0)

    assert(manager.emittedTaskSizeWarning)
  }

  def createTaskResult(id: Int): DirectTaskResult[Int] = {
    val valueSer = SparkEnv.get.serializer.newInstance()
    new DirectTaskResult[Int](valueSer.serialize(id), mutable.Map.empty, new TaskMetrics)
  }
}
