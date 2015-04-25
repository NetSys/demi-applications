import akka.actor.{ Actor, ActorRef, DeadLetter }
import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.actor.Props
import akka.dispatch.verification._
import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.util.Random
import pl.project13.scala.akka.raft.example._
import pl.project13.scala.akka.raft.protocol._
import pl.project13.scala.akka.raft.example.protocol._
import pl.project13.scala.akka.raft._
import pl.project13.scala.akka.raft.model._
import runner.raftchecks._
import runner.raftserialization._
import java.nio._

import scalax.collection.mutable.Graph,
       scalax.collection.GraphEdge.DiEdge,
       scalax.collection.edge.LDiEdge

class RaftMessageFingerprinter extends MessageFingerprinter {
  override def fingerprint(msg: Any) : Option[MessageFingerprint] = {
    val alreadyFingerprint = super.fingerprint(msg)
    if (alreadyFingerprint != None) {
      return alreadyFingerprint
    }

    def removeId(ref: ActorRef) : String = {
      return ref.path.name
    }
    val str = msg match {
      case RequestVote(term, ref, lastTerm, lastIdx) =>
        (("RequestVote", term, removeId(ref), lastTerm, lastIdx)).toString
      case LeaderIs(Some(ref), msg) =>
        ("LeaderIs", removeId(ref)).toString
      case m =>
        ""
    }

    if (str != "") {
      return Some(BasicFingerprint(str))
    }
    return None
  }
}

object Init {
  def actorCtor(): Props = {
    return Props.create(classOf[WordConcatRaftActor])
  }

  def startCtor(): Any = {
    val clusterRefs = Instrumenter().actorMappings.filter({
        case (k,v) => k != "client" && !ActorTypes.systemActor(k)
    }).values
    return ChangeConfiguration(ClusterConfiguration(clusterRefs))
  }

  // Very important! Need to update the actor refs recorded in the event
  // trace, since they are no longer valid for this new actor system.
  def updateActorRef(ref: ActorRef) : ActorRef = {
    val newRef = Instrumenter().actorSystem.actorFor("/user/" + ref.path.name)
    assert(newRef.path.name != "deadLetters")
    return newRef
  }

  def eventMapper(e: Event) : Option[Event] = {
    e match {
      case MsgSend(snd,rcv,ChangeConfiguration(config)) =>
        // Regenerate the ChangeConfiguration message with new ActorRefs.
        val updatedRefs = config.members.map(Init.updateActorRef)
        val updatedConfig = ChangeConfiguration(ClusterConfiguration(updatedRefs))
        return Some(MsgSend(snd,rcv,updatedConfig))
      case m =>
        return Some(m)
    }
  }
}

object Main extends App {
  val raftChecks = new RaftChecks

  val fingerprintFactory = new FingerprintFactory
  fingerprintFactory.registerFingerprinter(new RaftMessageFingerprinter)

  // -- Used for initial fuzzing: --
  val members = (1 to 9) map { i => s"raft-member-$i" }
  val prefix = Array[ExternalEvent]() ++
    members.map(member =>
      Start(Init.actorCtor, member)) ++
    members.map(member =>
      Send(member, Init.startCtor)) ++
    Array[ExternalEvent](WaitQuiescence()
  )
  // -- --

  def shutdownCallback() = {
    raftChecks.clear
  }

  Instrumenter().registerShutdownCallback(shutdownCallback)

  val mcs_dir = "experiments/akka-raft-fuzz_2015_04_19_15_35_23_IncDDMin_DPOR"
  var msgDeserializer = new RaftMessageDeserializer(Instrumenter().actorSystem)

  println("Trying replay..")
  RunnerUtils.replayExperiment(mcs_dir, fingerprintFactory, msgDeserializer,
                               Some(raftChecks.invariant),
                               traceFile=ExperimentSerializer.minimizedInternalTrace)
}
