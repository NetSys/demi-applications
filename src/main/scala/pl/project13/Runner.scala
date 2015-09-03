package akka.dispatch.verification

import akka.actor.{ Actor, ActorRef, DeadLetter }
import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.actor.Props
import akka.actor.FSM
import akka.actor.FSM.Timer
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
        //(("RequestVote", removeId(ref))).toString
      case LeaderIs(Some(ref), msg) =>
        ("LeaderIs", removeId(ref)).toString
      //case VoteCandidate(term) =>
      //  ("VoteCandidate").toString
      case m =>
        ""
    }

    if (str != "") {
      return Some(BasicFingerprint(str))
    }
    return None
  }

  // Does this message trigger a logical clock contained in subsequent
  // messages to be incremented?
  override def causesClockIncrement(msg: Any) : Boolean = {
    msg match {
      case Timer("election-timer", _, _, _) => return true
      case _ => return false
    }
  }

  // Extract a clock value from the contents of this message
  override def getLogicalClock(msg: Any) : Option[Long] = {
    msg match {
      case RequestVote(term, _, _, _) =>
        return Some(term.termNr)
      case AppendEntries(term, _, _, _, _) =>
        return Some(term.termNr)
      case VoteCandidate(term) =>
        return Some(term.termNr)
      case DeclineCandidate(term) =>
        return Some(term.termNr)
      case a: AppendResponse =>
        return Some(a.term.termNr)
      case _ => return None
    }
  }
}

class AppendWordConstuctor(word: String) extends ExternalMessageConstructor {
  def apply() : Any = {
    return ClientMessage[AppendWord](Instrumenter().actorSystem.deadLetters, AppendWord(word))
  }
}

class ClientMessageGenerator(raft_members: Seq[String]) extends MessageGenerator {
  var highestWordUsedSoFar = 0
  val rand = new Random
  val destinations = new RandomizedHashSet[String]
  for (dst <- raft_members) {
    destinations.insert(dst)
  }

  def generateMessage(alive: RandomizedHashSet[String]) : Send = {
    val dst = destinations.getRandomElement()
    var word = highestWordUsedSoFar.toString
    highestWordUsedSoFar += 1
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

  def externalMessageFilter(msg: Any) = {
    msg match {
      case ChangeConfiguration(_) => true
      case ClientMessage(_, _) => true
      case _ => false
    }
  }
}

object Main extends App {
  Instrumenter().setLogLevel("ERROR")
  EventTypes.setExternalMessageFilter(Init.externalMessageFilter)
  Instrumenter().setPassthrough
  Instrumenter().actorSystem
  Instrumenter().unsetPassthrough

  val raftChecks = new RaftChecks

  val fingerprintFactory = new FingerprintFactory
  fingerprintFactory.registerFingerprinter(new RaftMessageFingerprinter)

  // -- Used for initial fuzzing: --
  val members = (1 to 4) map { i => s"raft-member-$i" }
  val prefix = Array[ExternalEvent]() ++
    members.map(member =>
      Start(Init.actorCtor, member)) ++
    members.map(member =>
      Send(member, new BootstrapMessageConstructor(Set[Int]()))) ++
    Array[ExternalEvent](
      WaitCondition(() => LeaderTest.totalElected.get > 0))
      //Send("raft-member-3", new AppendWordConstuctor("WORD1")),
      //Send("raft-member-3", new AppendWordConstuctor("WORD2"))
    //)
    //Array[ExternalEvent](WaitQuiescence()
  //) XXX
  // -- --

  val rand = new Random(0)
  val toSchedule = new Queue[(String,String,String)]
  def userDefinedFilter(src: String, dst: String, msg: Any): Boolean = {
    toSchedule.synchronized {
      if (toSchedule.isEmpty) {
        msg match {
          case Timer("election-timer", _, _, _) =>
            if (rand.nextDouble < 0.70) {
              return false
            }
            return true
          case _ => return true
        }
      } else {
        // Force toSchedule to be scheduled
        val (s,d,m) = toSchedule.head
        if (src == s && dst == d && msg.toString.contains(m)) {
          toSchedule.dequeue
          return true
        }
        return false
      }
    }
  }
  def consensusInterleaving(): Boolean = {
    LeaderTest.consensusReached.synchronized {
      if (LeaderTest.consensusReached.size > 0) {
        toSchedule.synchronized {
          if (toSchedule.isEmpty) {
            val nextLeader = Instrumenter().actorMappings.keys.find(
              a => a != LeaderTest.consensusReached.keys.head &&
                   !ActorTypes.systemActor(a)).get
            toSchedule += (("deadLetters", nextLeader, "Timer(election-timer"))
            // TODO(cs): also add heartbeat from nextLeader -> oldLeader?
          }
        }
        return true
      }
    }
    return false
  }

  val postfix = Array[ExternalEvent](
    WaitCondition(consensusInterleaving))

  def shutdownCallback() = {
    raftChecks.clear
    LeaderTest.totalElected.set(0)
    LeaderTest.consensusReached.clear
  }

  Instrumenter().registerShutdownCallback(shutdownCallback)

  val schedulerConfig = SchedulerConfig(
    messageFingerprinter=fingerprintFactory,
    enableFailureDetector=false,
    enableCheckpointing=true,
    shouldShutdownActorSystem=true,
    filterKnownAbsents=false,
    invariant_check=Some(raftChecks.invariant),
    ignoreTimers=false
  )

  val weights = new FuzzerWeights(kill=0.00, send=0.3, wait_quiescence=0.0,
                                  partition=0.0, unpartition=0)
  val messageGen = new ClientMessageGenerator(members)
  val fuzzer = new Fuzzer(200, weights, messageGen, prefix, postfix=postfix)

  val fuzz = false

  var traceFound: EventTrace = null
  var violationFound: ViolationFingerprint = null
  var depGraph : Graph[Unique, DiEdge] = null
  var initialTrace : Queue[Unique] = null
  var filteredTrace : Queue[Unique] = null

  if (fuzz) {
    def replayerCtor() : ReplayScheduler = {
      val replayer = new ReplayScheduler(schedulerConfig)
      return replayer
    }

    def randomiziationCtor() : RandomizationStrategy = {
      //return new FullyRandom(userDefinedFilter)
      return new SrcDstFIFO(userDefinedFilter)
    }
    val tuple = RunnerUtils.fuzz(fuzzer, raftChecks.invariant,
                                 schedulerConfig,
                                 validate_replay=Some(replayerCtor),
                                 maxMessages=Some(3000),  // XXX
                                 invariant_check_interval=30,
                                 randomizationStrategyCtor=randomiziationCtor,
                                 computeProvenance=true)
    traceFound = tuple._1
    violationFound = tuple._2
    depGraph = tuple._3
    initialTrace = tuple._4
    filteredTrace = tuple._5
  }

  if (fuzz) {
    // XXX var provenanceTrace = traceFound.intersection(filteredTrace, fingerprintFactory)

    val serializer = new ExperimentSerializer(
      fingerprintFactory,
      new RaftMessageSerializer)

    val dir = serializer.record_experiment("akka-raft-fuzz-long",
      traceFound.filterCheckpointMessages(), violationFound,
      depGraph=Some(depGraph), initialTrace=Some(initialTrace),
      filteredTrace=Some(filteredTrace))

    val (mcs, stats1, verified_mcs, violation) =
    RunnerUtils.stsSchedDDMin(false,
      schedulerConfig,
      traceFound,
      violationFound,
      actorNameProps=Some(ExperimentSerializer.getActorNameProps(traceFound)))

    if (verified_mcs.isEmpty) {
      throw new RuntimeException("MCS wasn't validated")
    }

    val mcs_dir = serializer.serializeMCS(dir, mcs, stats1, verified_mcs, violation, false)
    println("MCS DIR: " + mcs_dir)
  } else { // !fuzz
    val dir =
    "experiments/akka-raft-fuzz-long_2015_08_30_21_57_21"
    val mcs_dir =
    "experiments/akka-raft-fuzz-long_2015_08_30_21_57_21_DDMin_STSSchedNoPeek"

    val msgSerializer = new RaftMessageSerializer
    val msgDeserializer = new RaftMessageDeserializer(Instrumenter()._actorSystem)

    def shouldRerunDDMin(externals: Seq[ExternalEvent]) =
      externals.exists({
        case s: Send => s.messageCtor.isInstanceOf[AppendWordConstuctor]
        case _ => false
      })

    RunnerUtils.runTheGamut(dir, mcs_dir, schedulerConfig, msgSerializer,
      msgDeserializer, shouldRerunDDMin=shouldRerunDDMin)
  }
}
