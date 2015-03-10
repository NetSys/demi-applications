import akka.actor._
import akka.dispatch.Dispatcher
import akka.dispatch.verification._
import scala.concurrent.duration._
import scala.collection.mutable.{HashSet, Stack, Queue, MutableList}
import scala.collection.immutable.HashMap
import scala.collection.MapLike
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import scala.annotation.tailrec
import java.util.concurrent.Semaphore
import scalax.collection.mutable.Graph,
       scalax.collection.GraphEdge.DiEdge,
       scalax.collection.edge.LDiEdge

object TestPings extends App {
  val actors = Array("ping1",
                     "ping2",
                     "ping3")
  val trace0 = Array[ExternalEvent]() ++
    actors.map(
      act => Start(() => Props[Pinger], act)) ++
    Array[ExternalEvent](
    Send("ping1", () => StartPinging("ping2")),
    Send("ping1", () => StopPinging)
  )

  val sched = new DPORwHeuristics(Some(6))
  Instrumenter().scheduler = sched
  val traceSem = new Semaphore(0)
  val traces = new MutableList[sched.Trace]
  val graph = Graph[Unique, DiEdge]()
  val graph2 = Graph[Unique, DiEdge]()
  graph.add(sched.getRootEvent)
  val pingEv1 = Unique(MsgEvent("deadLetters", "ping1", StartPinging("ping2")))
  val pingEv2 = Unique(MsgEvent("deadLetters", "ping1", StopPinging))
  graph.add(pingEv1)
  graph.add(pingEv2)
  graph.addEdge(sched.getRootEvent, pingEv1)(DiEdge)
  graph.addEdge(sched.getRootEvent, pingEv2)(DiEdge)
  val trace = new sched.Trace
  trace += sched.getRootEvent
  trace += pingEv1
  trace += pingEv2

  sched.run(trace0, 
            (q) => traces += (new sched.Trace ++ q),
            (g) => {
              graph2 ++= g
              traceSem.release
            },
            Some(trace),
            Some(graph))
  println("Returned to main, waiting")
  traceSem.acquire
  println("Done, explored " + traces.size)
}
