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
      case ChangeConfiguration(config) =>
        // Ignore contents of bootstrap messages; they should be the same for
        // all nodes in the system. This fingerprint is needed for shrinking
        // external message contents.
        "ChangeConfiguration"
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

class ClientMessageGenerator(raft_members: Seq[String]) extends MessageGenerator {
  class AppendWordConstuctor(word: String) extends ExternalMessageConstructor {
    def apply() : Any = {
      return ClientMessage[AppendWord](Instrumenter().actorSystem.deadLetters, AppendWord(word))
    }
  }

  val wordsUsedSoFar = new HashSet[String]
  val rand = new Random
  val destinations = new RandomizedHashSet[String]
  for (dst <- raft_members) {
    destinations.insert(dst)
  }

  def generateMessage(alive: RandomizedHashSet[String]) : Send = {
    val dst = destinations.getRandomElement()
    // TODO(cs): 10000 is a bit arbitrary, and this algorithm fails
    // disastrously as we start to approach 10000 Send events.
    var word = rand.nextInt(10000).toString
    while (wordsUsedSoFar contains word) {
      word = rand.nextInt(10000).toString
    }
    wordsUsedSoFar += word
    return Send(dst, new AppendWordConstuctor(word))
  }
}

case class BootstrapMessageConstructor(maskedIndices: Set[Int]) extends ExternalMessageConstructor {
  @scala.transient
  var components : Seq[ActorRef] = Seq.empty

  def apply(): Any = {
    val all = Instrumenter().actorMappings.filter({
                case (k,v) => k != "client" && !ActorTypes.systemActor(k)
              }).values.toSeq.sorted

     // TODO(cs): factor zipWithIndex magic into a static method in ExternalMessageConstructor.
     components = all.zipWithIndex.filterNot {
       case (e,i) => maskedIndices contains i
     }.map { case (e,i) => e }.toSeq

    return ChangeConfiguration(ClusterConfiguration(components))
  }

  override def getComponents() = components

  override def maskComponents(indices: Set[Int]) : ExternalMessageConstructor = {
    return new BootstrapMessageConstructor(indices ++ maskedIndices)
  }
}

object Init {
  def actorCtor(): Props = {
    return Props.create(classOf[WordConcatRaftActor])
  }

  // Very important! Need to update the actor refs recorded in the event
  // trace, since they are no longer valid for this new actor system.
  def updateActorRef(ref: ActorRef) : ActorRef = {
    val newRef = Instrumenter().actorSystem.actorFor("/user/" + ref.path.name)
    assert(newRef.path.name != "deadLetters")
    return newRef
  }
}

object Main extends App {
  val raftChecks = new RaftChecks

  val members = (1 to 9) map { i => s"raft-member-$i" }

  val prefix = Array[ExternalEvent]() ++
    members.map(member =>
      Start(Init.actorCtor, member)) ++
    members.map(member =>
      Send(member, new BootstrapMessageConstructor(Set[Int]()))) ++
    Array[ExternalEvent](WaitQuiescence()
  )

  val fingerprintFactory = new FingerprintFactory
  fingerprintFactory.registerFingerprinter(new RaftMessageFingerprinter)

  val weights = new FuzzerWeights(kill=0.01, send=0.3, wait_quiescence=0.1,
                                  partition=0.1, unpartition=0)
  val messageGen = new ClientMessageGenerator(members)
  val fuzzer = new Fuzzer(500, weights, messageGen, prefix)

  def shutdownCallback() = {
    raftChecks.clear
  }

  Instrumenter().registerShutdownCallback(shutdownCallback)

  val fuzz = false

  var traceFound: EventTrace = null
  var violationFound: ViolationFingerprint = null
  var depGraph : Graph[Unique, DiEdge] = null
  var initialTrace : Queue[Unique] = null
  var filteredTrace : Queue[Unique] = null
  if (fuzz) {
    def replayerCtor() : ReplayScheduler = {
      val replayer = new ReplayScheduler(fingerprintFactory, false, false)
      return replayer
    }
    val tuple = RunnerUtils.fuzz(fuzzer, raftChecks.invariant,
                                 fingerprintFactory,
                                 validate_replay=Some(replayerCtor),
                                 maxMessages=Some(500))
    traceFound = tuple._1
    violationFound = tuple._2
    depGraph = tuple._3
    initialTrace = tuple._4
    filteredTrace = tuple._5
  }

  val prefix_dir = "experiments/"
  val original = prefix_dir+"akka-raft-fuzz_2015_05_17_17_14_33"
  val mcs_shrunk = prefix_dir+"akka-raft-fuzz_2015_05_17_17_14_33_DDMin_STSSchedNoPeek_shrunk"

  var msgDeserializer = new RaftMessageDeserializer(Instrumenter().actorSystem)
  RunnerUtils.replayExperiment(mcs_shrunk, fingerprintFactory, msgDeserializer, Some(raftChecks.invariant),
                       traceFile=ExperimentSerializer.minimizedInternalTrace)
}
