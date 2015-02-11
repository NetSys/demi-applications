import akka.actor.{ Actor, ActorRef, DeadLetter }
import akka.actor.ActorSystem
import akka.actor.Props
import akka.dispatch.verification._
import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import pl.project13.scala.akka.raft.example._
import pl.project13.scala.akka.raft.protocol._
import pl.project13.scala.akka.raft.example.protocol._
import pl.project13.scala.akka.raft._

object Main extends App {
  val members = (1 to 3) map { i => s"raft-member-$i" }

  val trace = Array[ExternalEvent]() ++
    Array[ExternalEvent](Start(() =>
      RaftClientActor.props(Instrumenter().actorSystem() / "raft-member-*"), "client")) ++
    members.map(member =>
      Start(() => Props.create(classOf[WordConcatRaftActor]), member)) ++
    members.map(member =>
      Send(member, () => {
        val clusterRefs = Instrumenter().actorMappings.filter({case (k,v) => k != "client"})
        ChangeConfiguration(ClusterConfiguration(clusterRefs.values))
      })) ++
    Array[ExternalEvent](
    // Send("client", ClientMessage(AppendWord("I"))),
    // Send("client", ClientMessage(AppendWord("like"))),
    // Send("client", ClientMessage(AppendWord("capybaras"))),
    WaitQuiescence,
    WaitTimers(1),
    WaitQuiescence,
    Continue(500)
    // TODO(cs): I think I might need to create an actor other than the client that sends these requests...
    // Send("client", ClientMessage(GetWords)),
  )

  val sched = new RandomScheduler(3, false)
  Instrumenter().scheduler = sched
  sched.explore(trace)
  println("Returned to main with events")
}
