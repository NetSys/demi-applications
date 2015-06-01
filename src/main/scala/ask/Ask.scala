package ask;

import akka.actor.{ Actor, ActorRef }
import akka.actor.{ ActorSystem, Scheduler, Props, Cancellable }
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

// XXX
import akka.dispatch.verification._

case object Question
case object GodSaysAsk

class Asker() extends Actor {

  def receive = {
    case GodSaysAsk =>
      println("asking question")
      implicit val timeout = Timeout(5 seconds)
      val fut = context.actorFor("../receiver") ? Question
      // XXX
      Instrumenter().actorBlocked()
      val res = Await.result(fut, timeout.duration).asInstanceOf[String]
      println("answer: " + res)

    case Question =>
      println("answering...")
      sender() ! "Fred"
  }
}
