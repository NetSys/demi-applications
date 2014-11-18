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

// Failure detector messages (the FD is perfect in this case)
final case class Killed (name: String) {}
final case class Started (name: String) {}
final case class GroupMembership (members: Iterable[String]) {}

object Test extends App {
  val actors = Array("bcast1",
                     "bcast2",
                     "bcast3",
                     "bcast4",
                     "bcast5",
                     "bcast6",
                     "bcast7",
                     "bcast8")

  val trace = Array[ExternalEvent](
    Start(Props[ReliableBCast], "bcast1"),
    Start(Props[ReliableBCast], "bcast2"),
    Start(Props[ReliableBCast], "bcast3"),
    Start(Props[ReliableBCast], "bcast4"),
    Start(Props[ReliableBCast], "bcast5"),
    Start(Props[ReliableBCast], "bcast6"),
    Start(Props[ReliableBCast], "bcast7"),
    Start(Props[ReliableBCast], "bcast8"),
    Send("bcast1", GroupMembership(actors)),
    Send("bcast2", GroupMembership(actors)),
    Send("bcast3", GroupMembership(actors)),
    Send("bcast4", GroupMembership(actors)),
    Send("bcast5", GroupMembership(actors)),
    Send("bcast6", GroupMembership(actors)),
    Send("bcast7", GroupMembership(actors)),
    Send("bcast8", GroupMembership(actors)),
    WaitQuiescence,
    Partition("bcast8", "bcast1"),
    Send("bcast5", Bcast(null, Msg("Foo", 1))),
    Partition("bcast8", "bcast2"),
    Partition("bcast8", "bcast3"),
    Partition("bcast8", "bcast4"),
    Partition("bcast8", "bcast5"),
    Partition("bcast8", "bcast6"),
    Partition("bcast8", "bcast7")
  )
  val sched = new PeekScheduler
  Instrumenter().scheduler = sched
  val events = sched.peek(trace)
  println("Returned to main with events")
  println("Shutting down")
  sched.shutdown
  println("Shutdown successful")
}
