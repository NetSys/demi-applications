import akka.actor._
import akka.dispatch.Dispatcher
import akka.dispatch.verification._
import scala.concurrent.duration._
import scala.collection.mutable.{OpenHashMap, HashSet, Stack}
import scala.collection.MapLike
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import scala.annotation.tailrec

// Control messages
final case class Start (prop: Props, name: String) {}
final case class Kill (name: String) {}
final case class Send (name: String, message: Any) {}
final case object EnvAck;
final case class Wait (duration: FiniteDuration)
final case class WaitQuiescence ()
final case class Verify (message: Any) {}

// Failure detector messages (the FD is perfect in this case)
final case class Killed (name: String) {}
final case class Started (name: String) {}
final case class GroupMembership (members: Iterable[String]) {}

// Test environment: responsible for executing external events.
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
      if (active contains name) {
        context.actorFor(name) ! msg
      }
      sender ! EnvAck
    case Terminated(a: ActorRef) =>
      active -= (a.path.name)
      val terminated = Killed(a.path.name)
      active.values.foreach(_ ! terminated)
    case Verify(verificationMsg) =>
      implicit val timeout = Timeout(5 seconds)
      val f = active.mapValues(_ ? verificationMsg)
      val cf = collection.immutable.Map(
              f.mapValues(Await.result(_, 1 second)).toSeq: _*)
      sender ! cf
  }
}

class TestDriver(env: ActorRef,
                 trace: Array[_ <: Any],
                 verificationMsg: Any,
                 verificationFun: Map[String, Any] => Boolean) {
  private[this] var verificationPhase = false
  def verify (): Boolean = {
    assert (verificationPhase)
    implicit val timeout = Timeout(5 seconds)
    val f = env ? new Verify(verificationMsg)
    val verification = Await.result(f, 5 seconds)
    val v = verification.asInstanceOf[collection.immutable.Map[String, Any]]
    val res =  verificationFun(v)
    return res

  }
  def run: Boolean = {
    println("Trace replay starting, trace is of length " + trace.size)
    for (t <- trace) {
      t match {
        // Remove this once we have more control over message sending and
        // delivery; in particular just wait for quiscence or something.
        case Wait(duration) =>
          Thread.sleep(duration.toMillis)
        case WaitQuiescence() =>
          //val finishedWait = dispatcher.awaitQuiscence()
        case _ =>
          implicit val timeout = Timeout(5 minutes)
          val f = env ? t
          Await.result(f, timeout.duration)
      }
    }
    verificationPhase = true
    println("Trace done")
    return verify()
  }
}

object BcastTest extends App {
  def run(input: Array[_ <: Any]): Boolean = {
    val sys = ActorSystem("PP", ConfigFactory.load())
    val env = sys.actorOf(Props[Environment], name="env")

    def messageVerify (a: Map[String, Any]) : Boolean = {
      println(a)
      val all_orders = a.values.map(_.asInstanceOf[List[Int]]).toList
      val first_order  = all_orders(0)
      var result = true
      for (order <- all_orders) {
        val t = first_order.equals(order)
        result = result & t
      }
      return result
    }
    
    val td = new TestDriver(env, 
                input, 
                MsgIds, 
                (a: Map[String, Any]) => messageVerify(a))
    
    val verify = td.run
    
    sys.shutdown()
    return verify
  }
  //@tailrec
  //def minimizeLoop(input: Array[_ <: Any], removed: Any, index: Int) {
    //val verify = run(input) 
    //// Does the bug still occur
    //var newInput: Array[_ <: Any] = null
    //var nindex = index
    //if (!verify) {
      //// Yes
      //println("Found bug")
      //newInput = input      
    //} else {
      //// No
      //println("Did not find bug")
      //nindex += 1
      //newInput = input.slice(0, index) ++ Array(removed) ++ input.slice(index, input.size)
    //}
    //if (nindex >= newInput.size) {
      //// Done
      //println("Done, final size is " + newInput.size)
      ////println("Trace is ")
      ////for (inp <- newInput) {
        ////println(inp)
      ////}
    //} else {
      //val actNewInput = newInput.slice(0, nindex) ++ newInput.slice(nindex + 1, newInput.size)
      //minimizeLoop(actNewInput, newInput(nindex), nindex)
    //}
  //}

  //def minimize(input: Array[_ <: Any]) = {
    //// First run the whole thing
    //val bug = run(input)
    //if (!bug) {
      //println("No problem found on the whole trace")
    //} else {
      //minimizeLoop(input.slice(1, input.size), input(0), 0)
    //}

  //}
  val trace = Array(
      Start(Props[ReliableBCast], "bcast1"),
      Start(Props[ReliableBCast], "bcast2"),
      Start(Props[ReliableBCast], "bcast3"),
      Start(Props[ReliableBCast], "bcast4"),
      WaitQuiescence(),
      Send ("bcast1", Bcast(null, Msg("hi", 1))),
      Send ("bcast2", Bcast(null, Msg("bye", 2))),
      Send ("bcast3", Bcast(null, Msg("bye", 3))),
      Send ("bcast4", Bcast(null, Msg("b", 4))),
      Start(Props[ReliableBCast], "bcast5"),
      Start(Props[ReliableBCast], "bcast6"),
      Start(Props[ReliableBCast], "bcast7"),
      Start(Props[ReliableBCast], "bcast8"),
      WaitQuiescence()
    )
  Instrumenter().scheduler = new TemplateScheduler
  run(trace)
}
