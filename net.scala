import akka.actor._
import scala.concurrent.duration._
import scala.collection.mutable.{OpenHashMap, HashSet}

final case class Start (prop: Props, name: String) {}
final case class Kill (name: String) {}
final case class Killed (name: String) {}
final case class Started (name: String) {}
final case class GroupMembership (members: Iterable[String]) {}

final class Msg(_msg: String, _id: Int) {
  val id = _id
  val msg = _msg
}
final case class Ack(from: String, id: Int) {}
final case class Bcast(from: String, msg: Msg) {}

class ReliableBCast extends Actor {
  private[this] val other = new HashSet[String]
  private[this] val msgIds = new HashSet[Int]
  private[this] val messages = new OpenHashMap[Int, Msg]
  private[this] val sendTo = new OpenHashMap[Int, HashSet[String]]
  private[this] val name = self.path.name
  private[this] def bcast(id:Int) = {
    val message = Bcast(name, messages(id))
    sendTo(id).foreach((act:String) => context.actorFor("../" + act) ! message)
  }
  def receive = {
    case GroupMembership(members) =>
      other ++= members
      // Nothing to bcast here, don't know what to do
    case Started(actor) =>
      other += actor
      // New actor, send messages
      for (m <- msgIds) {
        sendTo(m) += actor
        bcast(m)
      }
    case Killed(actor) =>
      other -= actor
    case Bcast(from, msg) =>
      context.actorFor("../" + from) ! Ack(name, msg.id)
      if (msgIds contains msg.id) {
        println("[" + name + "] Received dup broadcast from " + from + " id " + msg.id + " " + msg.msg)
        bcast(msg.id)
      } else {
        println("[" + name + "] Received broadcast from " + from + " id " + msg.id + " " + msg.msg)
        msgIds += msg.id
        messages += (msg.id -> msg)
        sendTo += (msg.id -> other.clone)
        sendTo(msg.id) -= from
        bcast(msg.id)
      }
    case Ack(from, id) =>
      sendTo(id) -= from
  }
}


class Environment extends Actor {
  private[this] val active = new OpenHashMap[String, ActorRef]
  def receive = {
    case o:Start =>
      val child = context.actorOf(o.prop, name=o.name)
      context.watch(child)
      val gm = GroupMembership(active.keys.toList)
      child ! gm
      val started = Started(o.name)
      active.values.foreach(_ ! started)
      active += (o.name -> child)
    case o:Kill =>
      if (active contains o.name) {
        context.stop(active(o.name))
      }
    case Terminated(a: ActorRef) =>
      active -= (a.path.name)
      if (active.keys.size == 0) {
        context.stop(self)
      } else {
        val terminated = Killed(a.path.name)
        active.values.foreach(_ ! terminated)
      }
  }
}

class TestDriver(env:ActorRef) extends Actor {
  run
  def run = {
    env ! Start(Props[ReliableBCast], "bcast1")
    val x = new Msg("Hello", 1)
    context.actorFor("/user/env/bcast1") ! Bcast("/user/td", x)
    env ! Start(Props[ReliableBCast], "bcast2")
    env ! Start(Props[ReliableBCast], "bcast3")
    env ! Start(Props[ReliableBCast], "bcast4")
    env ! Kill("bcast3")
  }
  def receive = {
    case _ => println("TD message")
  }
}

object BcastTest extends App {
  val sys = ActorSystem("PP")
  val env = sys.actorOf(Props[Environment], name="env")
  val td = sys.actorOf(Props(classOf[TestDriver], env), name="td")
}
