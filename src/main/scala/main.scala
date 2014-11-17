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

final case object Start
final case class Send(name: String, message: String, id: Int)
// Failure detector messages (the FD is perfect in this case)
final case class Killed (name: String) {}
final case class Started (name: String) {}
final case class GroupMembership (members: Iterable[String]) {}

class TestAgent(actors: Array[String])  extends Actor {
  def receive = {
    case Start =>
      for (a <- actors) {
        val actor = context.actorOf(Props[ReliableBCast], a)
        actor ! GroupMembership(actors)
      }
      context.actorFor(actors(0)) ! Bcast(null, Msg("Hello", 1)) 
    case Send(name, message, id) =>
      context.actorFor(name) ! Bcast(null, Msg(message, id))
  }
}


object Test extends App {
  val actors = Array("bcast1",
                     "bcast2",
                     "bcast3",
                     "bcast4",
                     "bcast5",
                     "bcast6",
                     "bcast7",
                     "bcast8")
  val partition = Set(
    ("bcast8", "bcast1"),
    ("bcast8", "bcast2"),
    ("bcast8", "bcast3"),
    ("bcast8", "bcast4"),
    ("bcast8", "bcast5"),
    ("bcast8", "bcast6"),
    ("bcast8", "bcast7")
  )
  val sched = new PartitioningFairScheduler
  Instrumenter().scheduler = sched
  sched.add_to_partition(partition)
  val sys = ActorSystem("TestAs", ConfigFactory.load())
  val initial = sys.actorOf(Props(classOf[TestAgent], actors), "initial")
  //initial ! Start
  val ret = sched.enqueue_message(initial, Start)
  require(ret)
  sched.enqueue_message(initial, Send("bcast5", "Hi", 2))
}
