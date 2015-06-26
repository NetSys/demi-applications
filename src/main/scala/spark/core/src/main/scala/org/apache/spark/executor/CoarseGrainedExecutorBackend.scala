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

import scala.concurrent.Await

import akka.actor.{Actor, ActorSelection, Props, ActorSystem, ActorRef}
import akka.pattern.Patterns
import akka.remote.{RemotingLifecycleEvent, DisassociatedEvent}

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkEnv}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.worker.WorkerWatcher
import org.apache.spark.scheduler.TaskDescription
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util.{AkkaUtils, SignalLogger, Utils}

import java.util.concurrent.Semaphore
import scala.concurrent.duration._
import akka.dispatch.verification._


private[spark] class CoarseGrainedExecutorBackend(
    driverUrl: String,
    executorId: String,
    hostPort: String,
    cores: Int,
    sparkProperties: Seq[(String, String)],
    doneSemaphore:Option[Semaphore]=None) extends Actor with ExecutorBackend with Logging {

  // Utils.checkHostPort(hostPort, "Expected hostport")

  var executor: Executor = null
  var driver: ActorRef = null

  def releaseSemaphore() {
    doneSemaphore match {
      case Some(sem) => sem.release
      case None =>
    }
  }

  override def preStart() {
    logInfo("Connecting to driver: " + driverUrl)
    //driver = context.actorSelection(driverUrl)
    driver = context.actorFor("../CoarseGrainedScheduler")
    driver ! RegisterExecutor(executorId, hostPort, cores)
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
  }

  override def receive = {
    case RegisteredExecutor =>
      logInfo("Successfully registered with driver")
      // Make this host instead of hostPort ?
      //executor = new Executor(executorId, Utils.parseHostPort(hostPort)._1, sparkProperties,
      //  true) XXX
      executor = new Executor(executorId, "localhost", sparkProperties,
        true)

    case RegisterExecutorFailed(message) =>
      logError("Slave registration failed: " + message)
      releaseSemaphore
      throw new RuntimeException("Slave registration failed: " + message)
      //System.exit(1)

    case LaunchTask(data) =>
      if (executor == null) {
        logError("Received LaunchTask command but executor was null")
        releaseSemaphore
        throw new RuntimeException("Received LaunchTask command but executor was null")
        //System.exit(1)
      } else {
        val ser = SparkEnv.get.closureSerializer.newInstance()
        val taskDesc = ser.deserialize[TaskDescription](data.value)
        logInfo("Got assigned task " + taskDesc.taskId)
        executor.launchTask(this, taskDesc.taskId, taskDesc.serializedTask)
      }

    case KillTask(taskId, _, interruptThread) =>
      if (executor == null) {
        logError("Received KillTask command but executor was null")
        releaseSemaphore
        throw new RuntimeException("Received KillTask command but executor was null")
      } else {
        executor.killTask(taskId, interruptThread)
      }

    case x: DisassociatedEvent =>
      logError(s"Driver $x disassociated! Shutting down.")
      releaseSemaphore
      throw new RuntimeException(s"Driver $x disassociated! Shutting down.")

    case StopExecutor =>
      logInfo("Driver commanded a shutdown")
      releaseSemaphore
      context.stop(self)
      // context.system.shutdown()
  }

  override def statusUpdate(taskId: Long, state: TaskState, data: ByteBuffer) {
    driver ! StatusUpdate(executorId, taskId, state, data)
  }
}

private[spark] object CoarseGrainedExecutorBackend extends Logging {
  def runLocal(actorSystem: ActorSystem, executorId: String) {
    val driver = actorSystem.actorFor("/user/CoarseGrainedScheduler")
    val timeout = 30.seconds
    val fut = Patterns.ask(driver, RetrieveSparkProps, timeout)
    Instrumenter().actorBlocked
    val props = Await.result(fut, timeout).asInstanceOf[Seq[(String, String)]]

    val doneSemaphore = new Semaphore(0)
    actorSystem.actorOf(
      Props(classOf[CoarseGrainedExecutorBackend],
        "CoarseGrainedScheduler", executorId, executorId, 1, props,
        Some(doneSemaphore)),
      name = "executor"+executorId)

    // Await completion
    doneSemaphore.acquire()
  }

  def run(driverUrl: String, executorId: String, hostname: String, cores: Int,
    workerUrl: Option[String]) {

    SignalLogger.register(log)

    SparkHadoopUtil.get.runAsSparkUser { () =>
      // Debug code
      Utils.checkHost(hostname)

      // Bootstrap to fetch the driver's Spark properties.
      val executorConf = new SparkConf
      val (fetcher, _) = AkkaUtils.createActorSystem(
        "driverPropsFetcher", hostname, 0, executorConf, new SecurityManager(executorConf))
      throw new RuntimeException("driverUrl:"+driverUrl)
      val driver = fetcher.actorSelection(driverUrl)
      val timeout = AkkaUtils.askTimeout(executorConf)
      val fut = Patterns.ask(driver, RetrieveSparkProps, timeout)
      Instrumenter().actorBlocked
      val props = Await.result(fut, timeout).asInstanceOf[Seq[(String, String)]]
      fetcher.shutdown()

      // Create a new ActorSystem using driver's Spark properties to run the backend.
      val driverConf = new SparkConf().setAll(props)
      val (actorSystem, boundPort) = AkkaUtils.createActorSystem(
        "sparkExecutor", hostname, 0, driverConf, new SecurityManager(driverConf))
      // set it
      val sparkHostPort = hostname + ":" + boundPort
      actorSystem.actorOf(
        Props(classOf[CoarseGrainedExecutorBackend],
          driverUrl, executorId, sparkHostPort, cores, props),
        name = "Executor")
      workerUrl.foreach { url =>
        actorSystem.actorOf(Props(classOf[WorkerWatcher], url), name = "WorkerWatcher")
      }
      actorSystem.awaitTermination()
    }
  }

  def main(args: Array[String]) {
    args.length match {
      case x if x < 4 =>
        System.err.println(
          // Worker url is used in spark standalone mode to enforce fate-sharing with worker
          "Usage: CoarseGrainedExecutorBackend <driverUrl> <executorId> <hostname> " +
          "<cores> [<workerUrl>]")
        System.exit(1)
      case 4 =>
        run(args(0), args(1), args(2), args(3).toInt, None)
      case x if x > 4 =>
        run(args(0), args(1), args(2), args(3).toInt, Some(args(4)))
    }
  }
}
