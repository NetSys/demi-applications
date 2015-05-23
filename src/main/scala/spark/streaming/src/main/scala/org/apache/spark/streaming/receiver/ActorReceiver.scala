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

package org.apache.spark.streaming.receiver

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.reflect.ClassTag

import akka.actor._
import akka.actor.SupervisorStrategy.{Escalate, Restart}
import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.storage.StorageLevel
import java.nio.ByteBuffer
import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * A helper with set of defaults for supervisor strategy
 */
@DeveloperApi
object ActorSupervisorStrategy {

  val defaultStrategy = OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange =
    15 millis) {
    case _: RuntimeException => Restart
    case _: Exception => Escalate
  }
}

/**
 * :: DeveloperApi ::
 * A receiver trait to be mixed in with your Actor to gain access to
 * the API for pushing received data into Spark Streaming for being processed.
 *
 * Find more details at: http://spark.apache.org/docs/latest/streaming-custom-receivers.html
 *
 * @example {{{
 *  class MyActor extends Actor with ActorHelper{
 *      def receive {
 *          case anything: String => store(anything)
 *      }
 *  }
 *
 *  // Can be used with an actorStream as follows
 *  ssc.actorStream[String](Props(new MyActor),"MyActorReceiver")
 *
 * }}}
 *
 * @note Since Actor may exist outside the spark framework, It is thus user's responsibility
 *       to ensure the type safety, i.e parametrized type of push block and InputDStream
 *       should be same.
 */
@DeveloperApi
trait ActorHelper {

  self: Actor => // to ensure that this can be added to Actor classes only

  /** Store an iterator of received data as a data block into Spark's memory. */
  def store[T](iter: Iterator[T]) {
    println("Storing iterator")
    context.parent ! IteratorData(iter)
  }

  /**
   * Store the bytes of received data as a data block into Spark's memory. Note
   * that the data in the ByteBuffer must be serialized using the same serializer
   * that Spark is configured to use.
   */
  def store(bytes: ByteBuffer) {
    context.parent ! ByteBufferData(bytes)
  }

  /**
   * Store a single item of received data to Spark's memory.
   * These single items will be aggregated together into data blocks before
   * being pushed into Spark's memory.
   */
  def store[T](item: T) {
    println("Storing item")
    context.parent ! SingleItemData(item)
  }
}

/**
 * :: DeveloperApi ::
 * Statistics for querying the supervisor about state of workers. Used in
 * conjunction with `StreamingContext.actorStream` and
 * [[org.apache.spark.streaming.receiver.ActorHelper]].
 */
@DeveloperApi
case class Statistics(numberOfMsgs: Int,
  numberOfWorkers: Int,
  numberOfHiccups: Int,
  otherInfo: String)

/** Case class to receive data sent by child actors */
private[streaming] sealed trait ActorReceiverData
private[streaming] case class SingleItemData[T](item: T) extends ActorReceiverData
private[streaming] case class IteratorData[T](iterator: Iterator[T]) extends ActorReceiverData
private[streaming] case class ByteBufferData(bytes: ByteBuffer) extends ActorReceiverData

/**
 * Provides Actors as receivers for receiving stream.
 *
 * As Actors can also be used to receive data from almost any stream source.
 * A nice set of abstraction(s) for actors as receivers is already provided for
 * a few general cases. It is thus exposed as an API where user may come with
 * his own Actor to run as receiver for Spark Streaming input source.
 *
 * This starts a supervisor actor which starts workers and also provides
 * [http://doc.akka.io/docs/akka/snapshot/scala/fault-tolerance.html fault-tolerance].
 *
 * Here's a way to start more supervisor/workers as its children.
 *
 * @example {{{
 *  context.parent ! Props(new Supervisor)
 * }}} OR {{{
 *  context.parent ! Props(new Worker, "Worker")
 * }}}
 */
private[streaming] class ActorReceiver[T: ClassTag](
    props: Props,
    name: String,
    storageLevel: StorageLevel,
    receiverSupervisorStrategy: SupervisorStrategy
  ) extends Receiver[T](storageLevel) with Logging {

  protected lazy val supervisor = SparkEnv.get.actorSystem.actorOf(Props(new Supervisor),
    "Supervisor" + streamId)

  class Supervisor extends Actor {

    override val supervisorStrategy = receiverSupervisorStrategy
    val worker = context.actorOf(props, name)
    logInfo("Started receiver worker at:" + worker.path)

    val n: AtomicInteger = new AtomicInteger(0)
    val hiccups: AtomicInteger = new AtomicInteger(0)

    def receive = {

      case IteratorData(iterator) =>
        println("received iterator")
        store(iterator.asInstanceOf[Iterator[T]])

      case SingleItemData(msg) =>
        println("received single")
        store(msg.asInstanceOf[T])
        n.incrementAndGet

      case ByteBufferData(bytes) =>
        store(bytes)

      case props: Props =>
        val worker = context.actorOf(props)
        logInfo("Started receiver worker at:" + worker.path)
        sender ! worker

      case (props: Props, name: String) =>
        val worker = context.actorOf(props, name)
        logInfo("Started receiver worker at:" + worker.path)
        sender ! worker

      case _: PossiblyHarmful => hiccups.incrementAndGet()

      case _: Statistics =>
        val workers = context.children
        sender ! Statistics(n.get, workers.size, hiccups.get, workers.mkString("\n"))

    }
  }

  def onStart() = {
    supervisor
    logInfo("Supervision tree for receivers initialized at:" + supervisor.path)

  }

  def onStop() = {
    supervisor ! PoisonPill
  }
}
