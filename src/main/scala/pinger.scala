import akka.actor._
import scala.concurrent.duration._
import scala.collection.mutable.{OpenHashMap, HashSet, Stack, ListBuffer, Queue}
import scala.collection.MapLike
import scala.concurrent.Await
import akka.pattern.ask
import akka.util.Timeout
import akka.dispatch.verification._

final case class StartPinging (actor: String)
final case object Ping
final case object Pong
final case object StopPinging

sealed trait State
case object Idle extends State
case object Pinging extends State

sealed trait Data
case object Undefined extends Data
case class PingActor (actor: String) extends Data

class Pinger extends FSM[State, Data] {
  startWith(Idle, Undefined)
  when(Idle) {
    case Event(StartPinging(actor), _) =>
      goto(Pinging) using PingActor(actor)
  }

  when(Pinging, stateTimeout = 0.5 seconds) {
    case Event(StopPinging, _) =>
      goto(Idle) using Undefined
    case Event(StateTimeout, PingActor(actor)) =>
      context.actorFor("../" + actor) ! Ping
      stay
  }

  whenUnhandled {
    case Event(Ping, _) => println(self.path.name + " Ping from " + sender.path.name)
                           sender ! Pong
                           stay
    case Event(Pong, _) => println(self.path.name + " Pong from " + sender.path.name)
                           stay
    case Event(_, _) => println("Unhandled event")
                        stay
  }

  onTransition {
    case Idle -> Pinging => println(self.path.name + " Starting to ping")
    case Pinging -> Idle => println(self.path.name + " stopping pings")
    case _ =>
    
  }

  initialize()
}
