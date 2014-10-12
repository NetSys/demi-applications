import akka.actor._
import scala.concurrent.duration._
import scala.collection.mutable.{OpenHashMap, HashSet, Stack}
import scala.collection.MapLike
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout

final case class Start (prop: Props, name: String) {}
final case class Kill (name: String) {}
final case class Send (name: String, message: Any) {}
final case object EnvAck;
final case class Wait (duration: FiniteDuration)
final case class Verify (message: Any) {}

final case class Killed (name: String) {}
final case class Started (name: String) {}
final case class GroupMembership (members: Iterable[String]) {}

final case class Msg(msg: String, id: Int) {}
final case class Ack(from: String, id: Int) {}
final case class Bcast(from: String, msg: Msg) {}
final case object MsgIds;

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
    case MsgIds =>
      sender ! msgIds
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
      sender ! EnvAck
    case o:Kill =>
      if (active contains o.name) {
        context.stop(active(o.name))
      }
      sender ! EnvAck
    case Send(name, msg) =>
      println("Received send message current members are " + active.keys.mkString(", "))
      if (active contains name) {
        println("Sending message")
        active(name) ! msg
      }
      sender ! EnvAck
    case Terminated(a: ActorRef) =>
      active -= (a.path.name)
      if (active.keys.size == 0) {
        context.stop(self)
      } else {
        val terminated = Killed(a.path.name)
        active.values.foreach(_ ! terminated)
      }
    case Verify(verificationMsg) =>
      implicit val timeout = Timeout(5 seconds)
      val f = active.mapValues(_ ? verificationMsg)
      val cf = collection.immutable.Map(
              f.mapValues(Await.result(_, 1 second)).toSeq: _*)
      sender ! cf
  }
}
class TestDriver(env: ActorRef,
                 trace: Stack[Any],
                 verificationMsg: Any,
                 verificationFun: Map[String, Any] => Boolean)
                 extends Actor {
  import context._
  run
  private[this] var verificationPhase = false
  def verify () = {
    assert (verificationPhase)
    implicit val timeout = Timeout(5 seconds)
    val f = env ? new Verify(verificationMsg)
    println("Starting to wait")
    val verification = Await.result(f, 5 seconds)
    val v = verification.asInstanceOf[collection.immutable.Map[String, Any]]
    //println(verification)
    assert(verificationFun(v))
    println("Verification succeeded, dying")
    context.stop(self)
    context.stop(env)
    
  }
  def run = {
    println("Trace replay starting")
    if (trace.isEmpty) {
      verificationPhase = true
      verify()
    }
    val obj = trace.pop()
    env ! obj

  }
  def receive = {
    case EnvAck =>
      if (trace.isEmpty) {
        verificationPhase = true
        verify()
      }
      else {
        val obj = trace.pop()
        obj match {
          case Wait(duration) =>
            // Use system dispatcher
            context.system.scheduler.scheduleOnce(duration, self, EnvAck)
          case _ => env ! obj
        }
      }
  }
}

object BcastTest extends App {
  val sys = ActorSystem("PP")
  val env = sys.actorOf(Props[Environment], name="env")
  val trace = new Stack[Any]
  trace.pushAll(
    List(
      Start(Props[ReliableBCast], "bcast1"),
      Send ("bcast1", Bcast("/user/td", Msg("hi", 1))),
      Wait(100 millis),
      Start(Props[ReliableBCast], "bcast2"),
      Wait(100 millis),
      Kill("bcast1"),
      Wait(100 millis),
      Start(Props[ReliableBCast], "bcast3"),
      Start(Props[ReliableBCast], "bcast4")
    ).reverse
  )
  private[this] def messageVerify (a: Map[String, Any]) = {
    val all_sets = List(a.values.map(_.asInstanceOf[HashSet[Int]]))
    val first_set  = all_sets(0)
    var result = true
    for (set <- all_sets) {
      result |= first_set.equals(all_sets)
    }
    result
  }
  val td = sys.actorOf(
            Props(classOf[TestDriver],
                  env,
                  trace,
                  MsgIds,
                  (a: Map[String, Any]) => messageVerify(a)),
                name="td")
}
