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

import java.util.Properties

import org.scalatest.FunSuite

import org.apache.spark._

class FakeSchedulerBackend extends SchedulerBackend {
  def start() {}
  def stop() {}
  def reviveOffers() {}
  def defaultParallelism() = 1
}

class FakeTaskSetManager(
    initPriority: Int,
    initStageId: Int,
    initNumTasks: Int,
    taskScheduler: TaskSchedulerImpl,
    taskSet: TaskSet)
  extends TaskSetManager(taskScheduler, taskSet, 0) {

  parent = null
  weight = 1
  minShare = 2
  priority = initPriority
  stageId = initStageId
  name = "TaskSet_"+stageId
  override val numTasks = initNumTasks
  tasksSuccessful = 0

  var numRunningTasks = 0
  override def runningTasks = numRunningTasks

  def increaseRunningTasks(taskNum: Int) {
    numRunningTasks += taskNum
    if (parent != null) {
      parent.increaseRunningTasks(taskNum)
    }
  }

  def decreaseRunningTasks(taskNum: Int) {
    numRunningTasks -= taskNum
    if (parent != null) {
      parent.decreaseRunningTasks(taskNum)
    }
  }

  override def addSchedulable(schedulable: Schedulable) {
  }

  override def removeSchedulable(schedulable: Schedulable) {
  }

  override def getSchedulableByName(name: String): Schedulable = {
    null
  }

  override def executorLost(executorId: String, host: String): Unit = {
  }

  override def resourceOffer(
      execId: String,
      host: String,
      maxLocality: TaskLocality.TaskLocality)
    : Option[TaskDescription] =
  {
    if (tasksSuccessful + numRunningTasks < numTasks) {
      increaseRunningTasks(1)
      Some(new TaskDescription(0, execId, "task 0:0", 0, null))
    } else {
      None
    }
  }

  override def checkSpeculatableTasks(): Boolean = {
    true
  }

  def taskFinished() {
    decreaseRunningTasks(1)
    tasksSuccessful +=1
    if (tasksSuccessful == numTasks) {
      parent.removeSchedulable(this)
    }
  }

  def abort() {
    decreaseRunningTasks(numRunningTasks)
    parent.removeSchedulable(this)
  }
}

class TaskSchedulerImplSuite extends FunSuite with LocalSparkContext with Logging {

  def createDummyTaskSetManager(priority: Int, stage: Int, numTasks: Int, cs: TaskSchedulerImpl,
      taskSet: TaskSet): FakeTaskSetManager = {
    new FakeTaskSetManager(priority, stage, numTasks, cs , taskSet)
  }

  def resourceOffer(rootPool: Pool): Int = {
    val taskSetQueue = rootPool.getSortedTaskSetQueue
    /* Just for Test*/
    for (manager <- taskSetQueue) {
       logInfo("parentName:%s, parent running tasks:%d, name:%s,runningTasks:%d".format(
         manager.parent.name, manager.parent.runningTasks, manager.name, manager.runningTasks))
    }
    for (taskSet <- taskSetQueue) {
      taskSet.resourceOffer("execId_1", "hostname_1", TaskLocality.ANY) match {
        case Some(task) =>
          return taskSet.stageId
        case None => {}
      }
    }
    -1
  }

  def checkTaskSetId(rootPool: Pool, expectedTaskSetId: Int) {
    assert(resourceOffer(rootPool) === expectedTaskSetId)
  }

  test("FIFO Scheduler Test") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskScheduler = new TaskSchedulerImpl(sc)
    val taskSet = FakeTask.createTaskSet(1)

    val rootPool = new Pool("", SchedulingMode.FIFO, 0, 0)
    val schedulableBuilder = new FIFOSchedulableBuilder(rootPool)
    schedulableBuilder.buildPools()

    val taskSetManager0 = createDummyTaskSetManager(0, 0, 2, taskScheduler, taskSet)
    val taskSetManager1 = createDummyTaskSetManager(0, 1, 2, taskScheduler, taskSet)
    val taskSetManager2 = createDummyTaskSetManager(0, 2, 2, taskScheduler, taskSet)
    schedulableBuilder.addTaskSetManager(taskSetManager0, null)
    schedulableBuilder.addTaskSetManager(taskSetManager1, null)
    schedulableBuilder.addTaskSetManager(taskSetManager2, null)

    checkTaskSetId(rootPool, 0)
    resourceOffer(rootPool)
    checkTaskSetId(rootPool, 1)
    resourceOffer(rootPool)
    taskSetManager1.abort()
    checkTaskSetId(rootPool, 2)
  }

  test("Fair Scheduler Test") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskScheduler = new TaskSchedulerImpl(sc)
    val taskSet = FakeTask.createTaskSet(1)

    val xmlPath = getClass.getClassLoader.getResource("fairscheduler.xml").getFile()
    System.setProperty("spark.scheduler.allocation.file", xmlPath)
    val rootPool = new Pool("", SchedulingMode.FAIR, 0, 0)
    val schedulableBuilder = new FairSchedulableBuilder(rootPool, sc.conf)
    schedulableBuilder.buildPools()

    assert(rootPool.getSchedulableByName("default") != null)
    assert(rootPool.getSchedulableByName("1") != null)
    assert(rootPool.getSchedulableByName("2") != null)
    assert(rootPool.getSchedulableByName("3") != null)
    assert(rootPool.getSchedulableByName("1").minShare === 2)
    assert(rootPool.getSchedulableByName("1").weight === 1)
    assert(rootPool.getSchedulableByName("2").minShare === 3)
    assert(rootPool.getSchedulableByName("2").weight === 1)
    assert(rootPool.getSchedulableByName("3").minShare === 0)
    assert(rootPool.getSchedulableByName("3").weight === 1)

    val properties1 = new Properties()
    properties1.setProperty("spark.scheduler.pool","1")
    val properties2 = new Properties()
    properties2.setProperty("spark.scheduler.pool","2")

    val taskSetManager10 = createDummyTaskSetManager(1, 0, 1, taskScheduler, taskSet)
    val taskSetManager11 = createDummyTaskSetManager(1, 1, 1, taskScheduler, taskSet)
    val taskSetManager12 = createDummyTaskSetManager(1, 2, 2, taskScheduler, taskSet)
    schedulableBuilder.addTaskSetManager(taskSetManager10, properties1)
    schedulableBuilder.addTaskSetManager(taskSetManager11, properties1)
    schedulableBuilder.addTaskSetManager(taskSetManager12, properties1)

    val taskSetManager23 = createDummyTaskSetManager(2, 3, 2, taskScheduler, taskSet)
    val taskSetManager24 = createDummyTaskSetManager(2, 4, 2, taskScheduler, taskSet)
    schedulableBuilder.addTaskSetManager(taskSetManager23, properties2)
    schedulableBuilder.addTaskSetManager(taskSetManager24, properties2)

    checkTaskSetId(rootPool, 0)
    checkTaskSetId(rootPool, 3)
    checkTaskSetId(rootPool, 3)
    checkTaskSetId(rootPool, 1)
    checkTaskSetId(rootPool, 4)
    checkTaskSetId(rootPool, 2)
    checkTaskSetId(rootPool, 2)
    checkTaskSetId(rootPool, 4)

    taskSetManager12.taskFinished()
    assert(rootPool.getSchedulableByName("1").runningTasks === 3)
    taskSetManager24.abort()
    assert(rootPool.getSchedulableByName("2").runningTasks === 2)
  }

  test("Nested Pool Test") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskScheduler = new TaskSchedulerImpl(sc)
    val taskSet = FakeTask.createTaskSet(1)

    val rootPool = new Pool("", SchedulingMode.FAIR, 0, 0)
    val pool0 = new Pool("0", SchedulingMode.FAIR, 3, 1)
    val pool1 = new Pool("1", SchedulingMode.FAIR, 4, 1)
    rootPool.addSchedulable(pool0)
    rootPool.addSchedulable(pool1)

    val pool00 = new Pool("00", SchedulingMode.FAIR, 2, 2)
    val pool01 = new Pool("01", SchedulingMode.FAIR, 1, 1)
    pool0.addSchedulable(pool00)
    pool0.addSchedulable(pool01)

    val pool10 = new Pool("10", SchedulingMode.FAIR, 2, 2)
    val pool11 = new Pool("11", SchedulingMode.FAIR, 2, 1)
    pool1.addSchedulable(pool10)
    pool1.addSchedulable(pool11)

    val taskSetManager000 = createDummyTaskSetManager(0, 0, 5, taskScheduler, taskSet)
    val taskSetManager001 = createDummyTaskSetManager(0, 1, 5, taskScheduler, taskSet)
    pool00.addSchedulable(taskSetManager000)
    pool00.addSchedulable(taskSetManager001)

    val taskSetManager010 = createDummyTaskSetManager(1, 2, 5, taskScheduler, taskSet)
    val taskSetManager011 = createDummyTaskSetManager(1, 3, 5, taskScheduler, taskSet)
    pool01.addSchedulable(taskSetManager010)
    pool01.addSchedulable(taskSetManager011)

    val taskSetManager100 = createDummyTaskSetManager(2, 4, 5, taskScheduler, taskSet)
    val taskSetManager101 = createDummyTaskSetManager(2, 5, 5, taskScheduler, taskSet)
    pool10.addSchedulable(taskSetManager100)
    pool10.addSchedulable(taskSetManager101)

    val taskSetManager110 = createDummyTaskSetManager(3, 6, 5, taskScheduler, taskSet)
    val taskSetManager111 = createDummyTaskSetManager(3, 7, 5, taskScheduler, taskSet)
    pool11.addSchedulable(taskSetManager110)
    pool11.addSchedulable(taskSetManager111)

    checkTaskSetId(rootPool, 0)
    checkTaskSetId(rootPool, 4)
    checkTaskSetId(rootPool, 6)
    checkTaskSetId(rootPool, 2)
  }

  test("Scheduler does not always schedule tasks on the same workers") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    // Need to initialize a DAGScheduler for the taskScheduler to use for callbacks.
    val dagScheduler = new DAGScheduler(sc, taskScheduler) {
      override def taskStarted(task: Task[_], taskInfo: TaskInfo) {}
      override def executorAdded(execId: String, host: String) {}
    }

    val numFreeCores = 1
    val workerOffers = Seq(new WorkerOffer("executor0", "host0", numFreeCores),
      new WorkerOffer("executor1", "host1", numFreeCores))
    // Repeatedly try to schedule a 1-task job, and make sure that it doesn't always
    // get scheduled on the same executor. While there is a chance this test will fail
    // because the task randomly gets placed on the first executor all 1000 times, the
    // probability of that happening is 2^-1000 (so sufficiently small to be considered
    // negligible).
    val numTrials = 1000
    val selectedExecutorIds = 1.to(numTrials).map { _ =>
      val taskSet = FakeTask.createTaskSet(1)
      taskScheduler.submitTasks(taskSet)
      val taskDescriptions = taskScheduler.resourceOffers(workerOffers).flatten
      assert(1 === taskDescriptions.length)
      taskDescriptions(0).executorId
    }
    val count = selectedExecutorIds.count(_ == workerOffers(0).executorId)
    assert(count > 0)
    assert(count < numTrials)
  }

  test("Scheduler correctly accounts for multiple CPUs per task") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskCpus = 2

    sc.conf.set("spark.task.cpus", taskCpus.toString)
    val taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    // Need to initialize a DAGScheduler for the taskScheduler to use for callbacks.
    val dagScheduler = new DAGScheduler(sc, taskScheduler) {
      override def taskStarted(task: Task[_], taskInfo: TaskInfo) {}
      override def executorAdded(execId: String, host: String) {}
    }
    taskScheduler.setDAGScheduler(dagScheduler)
    // Give zero core offers. Should not generate any tasks
    val zeroCoreWorkerOffers = Seq(new WorkerOffer("executor0", "host0", 0),
      new WorkerOffer("executor1", "host1", 0))
    val taskSet = FakeTask.createTaskSet(1)
    taskScheduler.submitTasks(taskSet)
    var taskDescriptions = taskScheduler.resourceOffers(zeroCoreWorkerOffers).flatten
    assert(0 === taskDescriptions.length)

    // No tasks should run as we only have 1 core free.
    val numFreeCores = 1
    val singleCoreWorkerOffers = Seq(new WorkerOffer("executor0", "host0", numFreeCores),
      new WorkerOffer("executor1", "host1", numFreeCores))
    taskScheduler.submitTasks(taskSet)
    taskDescriptions = taskScheduler.resourceOffers(singleCoreWorkerOffers).flatten
    assert(0 === taskDescriptions.length)

    // Now change the offers to have 2 cores in one executor and verify if it
    // is chosen.
    val multiCoreWorkerOffers = Seq(new WorkerOffer("executor0", "host0", taskCpus),
      new WorkerOffer("executor1", "host1", numFreeCores))
    taskScheduler.submitTasks(taskSet)
    taskDescriptions = taskScheduler.resourceOffers(multiCoreWorkerOffers).flatten
    assert(1 === taskDescriptions.length)
    assert("executor0" === taskDescriptions(0).executorId)
  }
}
