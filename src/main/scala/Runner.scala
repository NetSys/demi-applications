import ask._
import akka.actor.{ Actor, ActorRef, DeadLetter }
import akka.actor.ActorSystem
import akka.actor.Props
import akka.dispatch.verification._

object Main extends App {
  val prefix = Array[ExternalEvent](
     WaitCondition(() => false))

  val fingerprintFactory = new FingerprintFactory

  val sched = new RandomScheduler(1, fingerprintFactory, false, 0, false)
  def invariant(s: Seq[akka.dispatch.verification.ExternalEvent],
                c: scala.collection.mutable.HashMap[String,Option[akka.dispatch.verification.CheckpointReply]])
              : Option[akka.dispatch.verification.ViolationFingerprint] = {
    return None
  }
  sched.setInvariant(invariant)
  Instrumenter().scheduler = sched
  sched.nonBlockingExplore(prefix, (ret: Option[(EventTrace,ViolationFingerprint)]) => println("DONE!"))

  val recvRef = Instrumenter().actorSystem.actorOf(Props.create(classOf[Asker]), "receiver")
  val askRef = Instrumenter().actorSystem.actorOf(Props.create(classOf[Asker]), "asker")

  askRef ! GodSaysAsk
}
