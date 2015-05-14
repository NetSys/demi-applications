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

package org.apache.spark.executor

import java.nio.ByteBuffer
import org.apache.mesos.{Executor => MesosExecutor, MesosExecutorDriver, MesosNativeLibrary, ExecutorDriver}
import org.apache.mesos.Protos.{TaskState => MesosTaskState, TaskStatus => MesosTaskStatus, _}
import org.apache.spark.TaskState.TaskState
import com.google.protobuf.ByteString
import org.apache.spark.{Logging}
import org.apache.spark.TaskState
import org.apache.spark.util.Utils

private[spark] class MesosExecutorBackend
  extends MesosExecutor
  with ExecutorBackend
  with Logging {

  var executor: Executor = null
  var driver: ExecutorDriver = null

  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
    val mesosTaskId = TaskID.newBuilder().setValue(taskId.toString).build()
    driver.sendStatusUpdate(MesosTaskStatus.newBuilder()
      .setTaskId(mesosTaskId)
      .setState(TaskState.toMesos(state))
      .setData(ByteString.copyFrom(data))
      .build())
  }

  override def registered(
      driver: ExecutorDriver,
      executorInfo: ExecutorInfo,
      frameworkInfo: FrameworkInfo,
      slaveInfo: SlaveInfo) {
    logInfo("Registered with Mesos as executor ID " + executorInfo.getExecutorId.getValue)
    this.driver = driver
    val properties = Utils.deserialize[Array[(String, String)]](executorInfo.getData.toByteArray)
    executor = new Executor(
      executorInfo.getExecutorId.getValue,
      slaveInfo.getHostname,
      properties)
  }

  override def launchTask(d: ExecutorDriver, taskInfo: TaskInfo) {
    val taskId = taskInfo.getTaskId.getValue.toLong
    if (executor == null) {
      logError("Received launchTask but executor was null")
    } else {
      executor.launchTask(this, taskId, taskInfo.getData.asReadOnlyByteBuffer)
    }
  }

  override def error(d: ExecutorDriver, message: String) {
    logError("Error from Mesos: " + message)
  }

  override def killTask(d: ExecutorDriver, t: TaskID) {
    logWarning("Mesos asked us to kill task " + t.getValue + "; ignoring (not yet implemented)")
  }

  override def reregistered(d: ExecutorDriver, p2: SlaveInfo) {}

  override def disconnected(d: ExecutorDriver) {}

  override def frameworkMessage(d: ExecutorDriver, data: Array[Byte]) {}

  override def shutdown(d: ExecutorDriver) {}
}

/**
 * Entry point for Mesos executor.
 */
private[spark] object MesosExecutorBackend {
  def main(args: Array[String]) {
    MesosNativeLibrary.load()
    // Create a new Executor and start it running
    val runner = new MesosExecutorBackend()
    new MesosExecutorDriver(runner).run()
  }
}
