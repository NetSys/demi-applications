package akka.dispatch.verification

import com.typesafe.config.ConfigFactory
import akka.actor.{ActorCell, ActorRef, ActorSystem, Props}

import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.immutable.Set
import scala.collection.mutable.HashSet

import java.util.concurrent.Semaphore

abstract class TraceEvent
final case class Start (prop: Props, name: String) extends TraceEvent
final case class Kill (name: String) extends TraceEvent {}
final case class Send (name: String, message: Any) extends TraceEvent
final case object WaitQuiescence extends TraceEvent
final case class Partition (a: String, b: String) extends TraceEvent

// Just a very simple, non-null scheduler that supports 
// partitions.
class TraceFairScheduler()
    extends FairScheduler {

  // Pairs of actors that cannot communicate
  val partitioned = new HashSet[(String, String)]
  
  // An actor that is unreachable
  val inaccessible = new HashSet[String]

  // Actors to actor ref
  val actorToActorRef = new HashMap[String, ActorRef]

  var trace: Array[TraceEvent] = Array()
  var traceIdx: Int = 0
  var events: Queue[Event] = new Queue[Event]() 
  val traceSem = new Semaphore(0)

  // A set of messages to send
  val messagesToSend = new HashSet[(ActorRef, Any)]
  
  // Ensure exactly one thread in the scheduler at a time
  val schedSemaphore = new Semaphore(1)

  // TODO: Find a way to make this safe/move this into instrumenter
  val initialActorSystem = ActorSystem("initialas", ConfigFactory.load())

  def add_to_partition (newly_partitioned: Set[(String, String)]) {
    partitioned ++= newly_partitioned
  }
  def add_to_partition (newly_partitioned: (String, String)) {
    partitioned += newly_partitioned
  }

  def isolate_node (node: String) {
    inaccessible += node
  }

  def unisolate_node (node: String) {
    inaccessible -= node
  }

  def peek (_trace: Array[TraceEvent]) : Queue[Event]  = {
    trace = _trace
    events = new Queue[Event]()
    traceIdx = 0
    for (t <- trace) {
      t match {
        case Start (prop, name) => 
          // Just start and isolate all actors we might eventually care about
          initialActorSystem.actorOf(prop, name)
          isolate_node(name)
        case _ =>
          None
      }
    }
    advanceTrace()
    // Start waiting for trace to be done
    traceSem.acquire
    println("play_trace done")
    return events
  }

  private[this] def advanceTrace() {
    schedSemaphore.acquire
    var loop = true
    while (loop && traceIdx < trace.size) {
      trace(traceIdx) match {
        case Start (_, name) =>
          unisolate_node(name)
        case Kill (name) =>
          isolate_node(name)
        case Send (name, message) =>
          val res = enqueue_message(name, message)
          require(res)
        case Partition (a, b) =>
          add_to_partition((a, b))
        case WaitQuiescence =>
          loop = false // Start waiting for quiscence
      }
      traceIdx += 1
    }
    schedSemaphore.release
  }

  override def event_produced(cell: ActorCell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msgs = pendingEvents.getOrElse(rcv, new Queue[(ActorCell, Envelope)])

    if (!((partitioned contains (snd, rcv)) 
         || (partitioned contains (rcv, snd))
         || (inaccessible contains rcv) 
         || (inaccessible contains snd))) {
      pendingEvents(rcv) = msgs += ((cell, envelope))
    }
  }

  override def event_produced(event: Event) = {
    super.event_produced(event)
    event match { 
      case event : SpawnEvent => 
        actorToActorRef(event.name) = event.actor
    }
  }
  def enqueue_message(receiver: String, msg: Any) : Boolean  = {
    if (!(actorNames contains receiver)) {
      false
    } else {
      enqueue_message(actorToActorRef(receiver), msg)
    }
  }

  def enqueue_message(actor: ActorRef, msg: Any) : Boolean  = {
    if (instrumenter.started.get()) { 
      messagesToSend += ((actor, msg))
      true
    } else {
      actor ! msg
      true
    }
  }
  
  // Record that an event was consumed
  override def event_consumed(event: Event) = {
    events += event
  }
  
  override def event_consumed(cell: ActorCell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msg = envelope.message
    events += MsgEvent(snd, rcv, msg, cell, envelope)
  }

  override def schedule_new_message() : Option[(ActorCell, Envelope)] = {
    schedSemaphore.acquire
    for ((receiver, msg) <- messagesToSend) {
      receiver ! msg
    }
    messagesToSend.clear()
    instrumenter.await_enqueue()
    // schedule_new_message is reenterant, hence release before calling.
    schedSemaphore.release
    super.schedule_new_message()
  }

  override def notify_quiescence () {
    if (traceIdx < trace.size) {
      advanceTrace()
    } else {
      println("Done with events")
      traceSem.release
    }
  }
}
