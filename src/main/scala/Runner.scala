
import broadcast.FireStarter

import akka.actor.{ Actor, ActorRef, DeadLetter }
import akka.actor.ActorSystem
import akka.actor.Props


object Main extends App {
  //val sched = new PeekScheduler
  //Instrumenter().scheduler = sched

  val system = ActorSystem("Broadcast")
  val fireStarter = system.actorOf(
      Props(new FireStarter()),
      name="FS")
  fireStarter ! 0
}
