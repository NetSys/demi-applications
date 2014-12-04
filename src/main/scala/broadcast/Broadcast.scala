package broadcast;

import akka.actor.{ Actor, ActorRef }
import akka.actor.{ ActorSystem, Scheduler, Props }
import akka.pattern.ask
import akka.event.Logging
import akka.util.Timeout
import akka.cluster.VectorClock
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.parsing.json.JSONObject

// -- Initialization messages --
case class GroupMembership(members: Iterable[String])
case class SetVectorClock(vc: VectorClock)

// -- Application message type --
object DataMessage {
  // Global static variable to simplify creation of unique IDs.
  private var next_id = 0
  private def get_next_id = {next_id += 1; next_id}
}

case class DataMessage(data: String) {
  var id = DataMessage.get_next_id

  override
  def toString() : String = {
    "DataMessage(" + id + "," + data + ")"
  }
}

// -- main() -> Node messages --
case class Stop()
case class StillActiveQuery()
case class RBBroadcast(msg: DataMessage)

// -- Link -> Link messages --
case class SLDeliver(senderName: String, msg: DataMessage, vc: VectorClock)
case class ACK(senderName: String, msgID: Int, vc: VectorClock)

// -- Node -> Node messages --
case class Tick()

// -- FailureDetector -> Node messages --
case class SuspectedFailure(actor: ActorRef)
// N.B. even in a crash-stop failure model, SuspectedRecovery might still
// occur in the case that the FD realized that it made a mistake.
case class SuspectedRecovery(actor: ActorRef)

/**
 * FailureDetector interface.
 *
 * Guarentee: eventually all suspects are correctly suspected. We don't know
 * when that point will be though.
 *
 * We use an unorthodox "push" interface for notifying clients of suspected
 * failures, rather than the traditional "pull" interface. This is to achieve
 * quiescence.
 */
trait FailureDetector {}

/**
 * FailureDetector implementation meant to be integrated directly into a model checker or
 * testing framework. Doubles as a mechanism for killing nodes.
 */
// TODO(cs): change List[ActorRef] to List[String]
class HackyFailureDetector(nodes: List[ActorRef]) extends FailureDetector {
  var liveNodes : Set[ActorRef] = Set() ++ nodes

  def kill(node: ActorRef) {
    liveNodes = liveNodes - node
    node ! Stop
    val otherNodes = nodes.filter(n => n.compareTo(node) != 0)
    otherNodes.map(n => n ! SuspectedFailure(node))
  }

  // TODO(cs): support recovery. Upon recovering a node, send SuspectedRecovery messages to all links.
}

// Class variable for PerfectLink.
object PerfectLink {
  private val timerMillis = 500
}

/**
 * PerfectLink. Attached to BroadcastNodes.
 */
class PerfectLink(parent: BroadcastNode, destination: ActorRef, name: String) {
  var parentName = parent.name
  var destinationName = destination.path.name
  // Whether the destination is suspected to be crashed, according to a
  // FailureDetector.
  var destinationSuspected = false
  var delivered : Set[Int] = Set()
  var unacked : Map[Int,DataMessage] = Map()
  // Messages we refused to send b/c of suspected crash.
  var deliveryRefused : Map[Int,DataMessage] = Map()

  def pl_send(msg: DataMessage) {
    sl_send(msg)
  }

  def sl_send(msg: DataMessage) {
    if (destinationSuspected) {
      deliveryRefused += (msg.id -> msg)
      return
    }
    parent.vcLog("Sending SLDeliver(" + msg + ") to " + destinationName)
    destination ! SLDeliver(parentName, msg, parent.vc)
    if (unacked.size == 0) {
      parent.schedule_timer(PerfectLink.timerMillis)
    }
    unacked += (msg.id -> msg)
  }

  def handle_sl_deliver(senderName: String, msg: DataMessage, vc: VectorClock) {
    parent.vcLog("Received SLDeliver(" + msg + ") from " +
                 destinationName, otherVC=vc)
    parent.vcLog("Sending ACK(" + msg.id + ") to " + destinationName)
    destination ! ACK(parentName, msg.id, parent.vc)

    if (delivered contains msg.id) {
      return
    }

    delivered = delivered + msg.id
    parent.handle_pl_deliver(senderName, msg)
  }

  def handle_ack(senderName: String, msgID: Int, vc: VectorClock) {
    parent.vcLog("Received ACK(" + msgID + ") from " +
                 destinationName, otherVC=vc)
    unacked -= msgID
  }

  def handle_suspected_failure(suspect: ActorRef) {
    if (suspect.compareTo(destination) == 0) {
      parent.vcLog("Suspected crash of " + destinationName)
      destinationSuspected = true
    }
  }

  def handle_suspected_recovery(suspect: ActorRef) {
    if (suspect.compareTo(destination) == 0) {
      parent.vcLog("Suspected recovery of " + destinationName)
      destinationSuspected = false
      deliveryRefused.values.map(msg => sl_send(msg))
      unacked = unacked ++ deliveryRefused
      if (unacked.size != 0) {
        parent.schedule_timer(PerfectLink.timerMillis)
      }
      deliveryRefused = Map()
    }
  }

  def handle_tick() {
    if (destinationSuspected) {
      return
    }
    unacked.values.map(msg => sl_send(msg))
    if (unacked.size != 0) {
      parent.schedule_timer(PerfectLink.timerMillis)
    }
  }
}

/**
 * TimerQueue schedules timer events.
 */
class TimerQueue(scheduler: Scheduler, source: ActorRef) {
  var timerPending = false
  var active = true

  def maybe_schedule(timerMillis: Int) {
    if (timerPending) {
      return
    }
    timerPending = true
    active = true
    scheduler.scheduleOnce(
      timerMillis milliseconds,
      source,
      Tick)
  }

  def handle_tick() {
    timerPending = false
    active = false
  }
}

/**
 * BroadcastNode Actor. Implements Reliable Broadcast.
 */
class BroadcastNode extends Actor {
  var name = self.path.name
  val timerQueue = new TimerQueue(context.system.scheduler, self)
  var allLinks: Set[PerfectLink] = Set()
  var dst2link: Map[String, PerfectLink] = Map()
  var delivered: Set[Int] = Set()
  var vc = new VectorClock()
  val log = Logging(context.system, this)

  def handle_group_membership(group: Iterable[String]) {
    // TODO(cs): create a vector clock.
    group.map(node => add_link(node))
  }

  def add_link(dst: String) {
    val dst_ref = context.actorFor("../" + dst)
    val link = new PerfectLink(this, dst_ref, name + "-" + dst)
    allLinks = allLinks + link
    dst2link += (dst -> link)
  }

  def rb_broadcast(msg: DataMessage) {
    vcLog("Initiating RBBroadcast(" + msg + ")")
    beb_broadcast(msg)
  }

  def beb_broadcast(msg: DataMessage) {
    allLinks.map(link => link.pl_send(msg))
  }

  def handle_pl_deliver(senderName: String, msg: DataMessage) {
    handle_beb_deliver(senderName, msg)
  }

  def handle_beb_deliver(senderName: String, msg: DataMessage) {
    if (delivered contains msg.id) {
      return
    }

    delivered = delivered + msg.id
    vcLog("RBDeliver of message " + msg + " from " + senderName)
    beb_broadcast(msg)
  }

  def vcLog(msg: String, otherVC:VectorClock = null) {
    vc = vc :+ name
    if (otherVC != null) {
      vc = vc.merge(otherVC)
    }
    log.info(JSONObject(vc.versions).toString() + " " + msg)
  }

  def schedule_timer(timerMillis: Int) {
    timerQueue.maybe_schedule(timerMillis)
  }

  def stop() {
    vcLog("Crashing")
    context.stop(self)
  }

  def handle_tick() {
    vcLog("Handle Tick()")
    timerQueue.handle_tick
    allLinks.map(link => link.handle_tick)
  }

  def handle_suspected_failure(destination: ActorRef) {
    allLinks.map(link => link.handle_suspected_failure(destination))
  }

  def handle_suspected_recovery(destination: ActorRef) {
    allLinks.map(link => link.handle_suspected_recovery(destination))
  }

  def handle_active_query() {
    sender() ! timerQueue.active
  }

  def receive = {
    // Node messages:
    case GroupMembership(group) => handle_group_membership(group)
    case Stop => stop
    case RBBroadcast(msg) => rb_broadcast(msg)
    // Link messages:
    case SLDeliver(senderName, msg, vc) => {
      dst2link.getOrElse(senderName, null).handle_sl_deliver(senderName, msg, vc)
    }
    case ACK(senderName, msgID, vc) => {
      dst2link.getOrElse(senderName, null).handle_ack(senderName, msgID, vc)
    }
    // FailureDetector messages:
    case SuspectedFailure(destination) => handle_suspected_failure(destination)
    case SuspectedRecovery(destination) => handle_suspected_recovery(destination)
    case Tick => handle_tick
    case StillActiveQuery => handle_active_query
    case unknown => log.error("Unknown message " + unknown)
  }
}
