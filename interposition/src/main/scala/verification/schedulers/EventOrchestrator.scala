package akka.dispatch.verification

import com.typesafe.config.ConfigFactory
import akka.actor.{Actor, ActorCell, ActorRef, ActorSystem, Props}

import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.SynchronizedQueue
import scala.collection.immutable.Set
import scala.collection.mutable.HashSet

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Maintains the current state of the network, and provides
 * an interface for triggering events that affect that state, e.g. inducing
 * failures.
 *
 * Additionally records which events occured during the execution, possibly including internal events.
 *
 * Generic E specifies the type of event trace. Usually either Event or
 * ExternalEvent.
 */
class EventOrchestrator[E] {
  // Function that queues a message to be sent later.
  type EnqueueMessage = (String, Any) => Unit

  // Pairs of actors that cannot communicate
  val partitioned = new HashSet[(String, String)]

  // Actors that are unreachable
  val inaccessible = new HashSet[String]

  // Actors to actor ref
  // TODO(cs): make getter/setters for these
  val actorToActorRef = new HashMap[String, ActorRef]
  val actorToSpawnEvent = new HashMap[String, SpawnEvent]

  var trace: Seq[E] = List.empty[E]
  private[this] var traceIdx: Int = 0
  var events: Queue[Event] = new Queue[Event]()

  var fd : FDMessageOrchestrator = null

  def set_trace(_trace: Seq[E]) {
    trace = _trace
    events = new Queue[Event]()
    traceIdx = 0
  }

  def trace_advanced() = {
    traceIdx += 1
  }

  def trace_finished() : Boolean = {
    return traceIdx >= trace.size
  }

  def current_event() : E = {
    trace(traceIdx)
  }

  /**
   * Inform the given failure detector of any injected events in the future.
   */
  def set_failure_detector(_fd: FDMessageOrchestrator) = {
    fd = _fd
  }

  /**
   * Injects ExternalEvents in trace until Quiescence it's time for the
   * scheduler to wait for quiescence.
   */
  def inject_until_quiescence(enqueue_message: EnqueueMessage) = {
    var loop = true
    while (loop && !trace_finished) {
      current_event.asInstanceOf[ExternalEvent] match {
        case Start (_, name) =>
          trigger_start(name)
        case Kill (name) =>
          trigger_kill(name)
        case Send (name, message) =>
          enqueue_message(name, message)
        case Partition (a, b) =>
          trigger_partition(a,b)
        case UnPartition (a, b) =>
          trigger_unpartition(a,b)
        case WaitQuiescence =>
          loop = false // Start waiting for quiescence
      }
      trace_advanced()
    }
  }

  // Mark a couple of nodes as partitioned (so they cannot communicate)
  def add_to_partition (newly_partitioned: (String, String)) {
    partitioned += newly_partitioned
  }

  // Mark a couple of node as unpartitioned, i.e. they can communicate
  def remove_partition (newly_partitioned: (String, String)) {
    partitioned -= newly_partitioned
  }

  // Mark a node as unreachable, used to kill a node.
  // TODO(cs): to be implemented later: actually kill the node so that its state is cleared?
  def isolate_node (node: String) {
    inaccessible += node
  }

  // Mark a node as reachable, also used to start a node
  def unisolate_node (actor: String) {
    inaccessible -= actor
  }

  def trigger_start (name: String) = {
    events += actorToSpawnEvent(name)
    Util.logger.log(name, "God spawned me")
    unisolate_node(name)
    if (fd != null) {
      fd.handle_start_event(name)
    }
  }

  def trigger_kill (name: String) = {
    events += KillEvent(name)
    Util.logger.log(name, "God killed me")
    isolate_node(name)
    if (fd != null) {
      fd.handle_kill_event(name)
    }
  }

  def trigger_partition (a: String, b: String) = {
    events += PartitionEvent((a, b))
    add_to_partition((a, b))
    Util.logger.log(a, "God partitioned me from " + b)
    Util.logger.log(b, "God partitioned me from " + a)
    if (fd != null) {
      fd.handle_partition_event(a,b)
    }
  }

  def trigger_unpartition (a: String, b: String) = {
    events += UnPartitionEvent((a, b))
    remove_partition((a, b))
    Util.logger.log(a, "God reconnected me to " + b)
    Util.logger.log(b, "God reconnected me to " + a)
    if (fd != null) {
      fd.handle_unpartition_event(a,b)
    }
  }

  def handle_spawn_produced (event: SpawnEvent) = {
    actorToActorRef(event.name) = event.actor
    if (fd != null) {
      fd.create_node(event.name)
    }
  }

  def handle_spawn_consumed (event: SpawnEvent) = {
    actorToSpawnEvent(event.name) = event
  }

  def crosses_partition (snd: String, rcv: String) : Boolean = {
    return ((partitioned contains (snd, rcv))
           || (partitioned contains (rcv, snd))
           || (inaccessible contains rcv)
           || (inaccessible contains snd))
  }
}
