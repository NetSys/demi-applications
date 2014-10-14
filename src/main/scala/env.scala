import akka.actor._
import scala.concurrent.duration._
import scala.collection.mutable.{OpenHashMap, HashSet, Stack}
import scala.collection.MapLike
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

// Control messages
final case class Start (prop: Props, name: String) {}
final case class Kill (name: String) {}
final case class Send (name: String, message: Any) {}
final case object EnvAck;
final case class Wait (duration: FiniteDuration)
final case class Verify (message: Any) {}

// Failure detector messages (the FD is perfect in this case)
final case class Killed (name: String) {}
final case class Started (name: String) {}
final case class GroupMembership (members: Iterable[String]) {}

// Test environment: responsible for external events.
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
  val sys = ActorSystem("PP", ConfigFactory.load())
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
      Start(Props[ReliableBCast], "bcast4"),
      Start(Props[ReliableBCast], "bcast5"),
      Start(Props[ReliableBCast], "bcast6"),
      Start(Props[ReliableBCast], "bcast7"),
      Start(Props[ReliableBCast], "bcast8")
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
