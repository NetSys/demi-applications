import akka.actor._
import scala.concurrent.duration._
import scala.collection.mutable.{OpenHashMap, HashSet, Stack, ListBuffer, Queue}
import scala.collection.MapLike
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout
import akka.dispatch.verification._


// Messages for the reliable broadcast
final case class Msg(msg: String, id: Int) {}
final case class Ack(from: String, id: Int) {}
final case class Bcast(from: String, msg: Msg) {}
final case object MsgIds;

class BCastState {
  val messages = new Queue[Msg]
  def reset  = {
    messages.clear()
  }
}

class ReliableBCast (state: BCastState) extends Actor {
  private[this] val other = new HashSet[String]
  private[this] val msgIds = new HashSet[Int]
  private[this] val msgOrder = new ListBuffer[Int]
  private[this] val messages = new OpenHashMap[Int, Msg]
  private[this] val sendTo = new OpenHashMap[Int, HashSet[String]]
  private[this] val name = self.path.name
  private[this] var fdName : String = null

  // Broadcast a message by sending it to all unacked actors.
  private[this] def bcast(id:Int) = {
    val message = Bcast(name, messages(id))
    sendTo(id).foreach((act:String) => context.actorFor("../" + act) ! message)
  }
  def receive = {
    case FailureDetectorOnline(fdnode) =>
      fdName = fdnode
      context.actorFor("../" + fdName) ! QueryReachableGroup
    case ReachableGroup(members) =>
      // Just started, someone is providing us the lay of the land.
      other ++= members
      // Nothing to bcast here, don't know what to do
    case NodeReachable(actor) =>
      // A new actor was started
      other += actor
      //// New actor, send messages
      for (m <- msgIds) {
        sendTo(m) += actor
        bcast(m)
      }
    case Bcast(from, msg) =>
      println(name + " BCast, actors are " + other)
      println(name + " BCast from " + from)
      if (from != null) {
        context.actorFor("../" + from) ! Ack(name, msg.id)
      }
      if (msgIds contains msg.id) {
        bcast(msg.id)
      } else {
        state.messages += msg
        msgIds += msg.id
        msgOrder += msg.id
        messages += (msg.id -> msg)
        sendTo += (msg.id -> other.clone)
        sendTo(msg.id) -= from
        bcast(msg.id)
      }
    case Ack(from, id) =>
      sendTo(id) -= from
    case MsgIds =>
      sender ! msgOrder.toList
  }
}
