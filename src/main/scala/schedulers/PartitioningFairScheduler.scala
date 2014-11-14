package akka.dispatch.verification

import akka.actor.ActorCell

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
}
