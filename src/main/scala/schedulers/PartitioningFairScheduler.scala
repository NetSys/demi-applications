package akka.dispatch.verification

import akka.actor.{ActorCell, ActorRef}

import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.immutable.Set
import scala.collection.mutable.HashSet

// Just a very simple, non-null scheduler that supports 
// partitions.
class PartitioningFairScheduler()
    extends FairScheduler {
  val partitioned = new HashSet[(String, String)]
  val actorToActorRef = new HashMap[String, ActorRef]
  val messagesToSend = new HashSet[(ActorRef, Any)]
  def add_to_partition (newly_partitioned: Set[(String, String)]) {
    partitioned ++= newly_partitioned
  }
  def add_to_partition (newly_partitioned: (String, String)) {
    partitioned += newly_partitioned
  }
  override def event_produced(cell: ActorCell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msgs = pendingEvents.getOrElse(rcv, new Queue[(ActorCell, Envelope)])

    if (!((partitioned contains (snd, rcv)) || (partitioned contains (rcv, snd)))) {
      pendingEvents(rcv) = msgs += ((cell, envelope))
    }
  }

  override def event_produced(event: Event) = {
    super.event_produced(event)
    event match { 
      case event : SpawnEvent => 
        if (!(actorNames contains event.name)) {
          actorToActorRef(event.name) = event.actor
        }
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

  override def schedule_new_message() : Option[(ActorCell, Envelope)] = {
    for ((receiver, msg) <- messagesToSend) {
      println("Trying to enqueue messages")
      receiver ! msg
    }
    messagesToSend.clear()
    instrumenter.await_enqueue()
    super.schedule_new_message()
  }
}
