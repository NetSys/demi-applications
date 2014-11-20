import akka.actor._
import akka.dispatch.Dispatcher
import akka.dispatch.verification._
import scala.concurrent.duration._
import scala.collection.mutable.{HashSet, Stack, Queue, MutableList, HashMap}
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
  // Analyze a trace to figure out causal links. Quiescent states are currently
  // treated as sources and sinks.
  def analyzeTrace (eventTrace: Queue[Event]) {
    var currentContext = "scheduler"
    var currentStep = 1
    val enabledByStep = new Queue[((Int, String, Any),  List[Event])]
    var lastRecv : MsgEvent = null
    var lastMsg : Any = null
    var lastQuiescent = 0
    var list = new MutableList[Event]
    val causalLinks = new HashMap[Int, Queue[Int]]
    val messageSendTime = new HashMap[(String, String, Any), Int]
    val eventsSinceQuiescence = new Queue[Int]
    causalLinks(currentStep) = new Queue[Int]
    for (ev <- eventTrace) {
      ev match {
        case s: SpawnEvent =>
          list += s
        case m: MsgSend =>
          messageSendTime((m.sender, m.receiver, m.msg)) = currentStep
          list += m
        case ChangeContext(actor) =>
          if (currentContext != "scheduler" || list.size > 0 || lastMsg == Quiescence) {
            if (currentContext == "scheduler" && lastMsg != Quiescence) {
              // Everything the scheduler does must causally happen after the last quiescent
              // period
              causalLinks(currentStep) += lastQuiescent
            }
            eventsSinceQuiescence += currentStep
            enabledByStep += (((currentStep, currentContext, lastMsg), list.toList))
            currentStep += 1
          }
          list.clear()
          currentContext = actor
          lastMsg = null
          causalLinks(currentStep) = new Queue[Int]
          if (lastRecv != null) {
            require(lastRecv.receiver == currentContext)
            lastMsg = lastRecv.msg
            causalLinks(currentStep) += 
                messageSendTime((lastRecv.sender, lastRecv.receiver, lastRecv.msg))
            lastRecv = null
          }
        case e: MsgEvent =>
          lastRecv = e

        case Quiescence =>
          lastMsg = Quiescence
          lastQuiescent = currentStep
          causalLinks(currentStep) ++= eventsSinceQuiescence
          eventsSinceQuiescence.clear()
        case _ =>
          ()
      }
    }
    enabledByStep += (((currentStep, currentContext, lastMsg), list.toList))
    println("============================================================================")
    for(((time, actor, msg), queue) <- enabledByStep) {
      if (actor != "scheduler" || list.size > 0 || lastMsg == Quiescence) {
        println(time + " " + actor + " ↢ " + msg + "  [" + causalLinks(time) + "]")
        for (en <- queue) {
          println("\t" + en)
        }
      }
    }
    println("============================================================================")
  }

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
  //while (true) {
    val sched = new PeekScheduler
    Instrumenter().scheduler = sched
    val events = sched.peek(trace)
    println("Returned to main with events")
    println("Shutting down")
    sched.shutdown
    println("Shutdown successful")
    println("Now trying replay")
    val replaySched = new ReplayScheduler
    Instrumenter().scheduler = replaySched
    val eventsAsArray = events.toArray
    val replayedEvents = replaySched.replay(eventsAsArray)
    println("Done replaying")
    println("Shutting down")
    replaySched.shutdown
    println("Shutdown successful")
    analyzeTrace(replayedEvents)
}
