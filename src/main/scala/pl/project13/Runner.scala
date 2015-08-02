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
      Send(member, new BootstrapMessageConstructor(Set[Int]())))
    Array[ExternalEvent](WaitQuiescence()
  )
  // -- --

  def shutdownCallback() = {
    raftChecks.clear
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
  val fuzzer = new Fuzzer(0, weights, messageGen, prefix)

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
    val tuple = RunnerUtils.fuzz(fuzzer, raftChecks.invariant,
                                 schedulerConfig,
                                 validate_replay=Some(replayerCtor),
                                 maxMessages=Some(170)) // XXX
    traceFound = tuple._1
    violationFound = tuple._2
    depGraph = tuple._3
    initialTrace = tuple._4
    filteredTrace = tuple._5
  }

  if (fuzz) {
    var provenanceTrace = traceFound.intersection(filteredTrace, fingerprintFactory)

    val serializer = new ExperimentSerializer(
      fingerprintFactory,
      new RaftMessageSerializer)

    val dir = serializer.record_experiment("akka-raft-fuzz4",
      traceFound.filterCheckpointMessages(), violationFound,
      depGraph=Some(depGraph), initialTrace=Some(initialTrace),
      filteredTrace=Some(filteredTrace))

    val (mcs, stats, verified_mcs, violation) =
    RunnerUtils.stsSchedDDMin(false,
      schedulerConfig,
      provenanceTrace,
      violationFound,
      actorNameProps=Some(ExperimentSerializer.getActorNameProps(traceFound)))

    val mcs_dir = serializer.serializeMCS(dir, mcs, stats, verified_mcs, violation, false)

    // Actually try minimizing twice, to make it easier to understand what's
    // going on during each "Ignoring next" run.
    val (intMinStats, intMinTrace) = RunnerUtils.minimizeInternals(schedulerConfig,
                          mcs,
                          verified_mcs.get,
                          ExperimentSerializer.getActorNameProps(traceFound),
                          violationFound)

    RunnerUtils.printMinimizationStats(
      traceFound, Some(filteredTrace), verified_mcs.get, intMinTrace, schedulerConfig.messageFingerprinter)

    // Second minimization
    val (intMinStats2, intMinTrace2) = RunnerUtils.minimizeInternals(schedulerConfig,
                          mcs,
                          intMinTrace,
                          ExperimentSerializer.getActorNameProps(traceFound),
                          violationFound)

    RunnerUtils.printMinimizationStats(
      traceFound, Some(filteredTrace), verified_mcs.get, intMinTrace2, schedulerConfig.messageFingerprinter)

    serializer.recordMinimizedInternals(mcs_dir, intMinStats2, intMinTrace2)
    println("MCS DIR: " + mcs_dir)
  } else { // !fuzz
    val dir = "/Users/cs/Research/UCB/code/sts2-applications/experiments/akka-raft-fuzz4_2015_08_01_16_26_01_DDMin_STSSchedNoPeek"

    val deserializer = new ExperimentDeserializer(dir)
    val msgDeserializer = new RaftMessageDeserializer(Instrumenter()._actorSystem)
    val replayTrace = RunnerUtils.replayExperiment(dir,
      schedulerConfig, msgDeserializer, traceFile=ExperimentSerializer.minimizedInternalTrace)

    println("replayTrace:")
    replayTrace.events.foreach {
      case e => println(e)
    }
    println("---")

    println("initial devlieries:")
    RunnerUtils.printDeliveries(replayTrace)
    println("---")

    val reordered = RunnerUtils.reorderDeliveries(replayTrace,
      Seq(
        // UniqueMsgSend(MsgSend(deadLetters,raft-member-4,ChangeConfiguration(StableRaftConfiguration(Set(raft-member-1,
        //   raft-member-2, raft-member-3, raft-member-4)))),3848)
        3848,
        // UniqueMsgEvent(MsgEvent(deadLetters,raft-member-3,ChangeConfiguration(StableRaftConfiguration(Set(raft-member-1,
        // raft-member-2, raft-member-3, raft-member-4)))),3847)
        3847,
        // UniqueMsgEvent(MsgEvent(deadLetters,raft-member-2,ChangeConfiguration(StableRaftConfiguration(Set(raft-member-1,
        //   raft-member-2, raft-member-3, raft-member-4)))),3846)
        3846,
        // UniqueMsgEvent(MsgEvent(deadLetters,raft-member-2,Timer(election-timer,ElectionTimeout,false,1)),3851)
        3851,
        // UniqueMsgEvent(MsgEvent(deadLetters,raft-member-3,Timer(election-timer,ElectionTimeout,false,1)),3850)
        3850,
        // UniqueMsgEvent(MsgEvent(raft-member-3,raft-member-3,BeginElection),3855)
        3855,
        // UniqueMsgEvent(MsgEvent(raft-member-3,raft-member-4,RequestVote(Term(1),
        //  Actor[akka://new-system-0/user/raft-member-3#152274668],Term(0),0)),3861)
        3861,
        // UniqueMsgEvent(MsgEvent(deadLetters,raft-member-3,Timer(election-timer,ElectionTimeout,false,3)),3856)
        3856,
        // UniqueMsgEvent(MsgEvent(raft-member-4,raft-member-3,VoteCandidate(Term(0))),3862)
        3862,
        // UniqueMsgEvent(MsgEvent(raft-member-3,raft-member-3,BeginElection),3863)
        3863,
        // UniqueMsgEvent(MsgEvent(raft-member-3,raft-member-4,RequestVote(Term(2),
        //  Actor[akka://new-system-0/user/raft-member-3#152274668],Term(0),0)),3866)
        3866,
        // UniqueMsgEvent(MsgEvent(deadLetters,raft-member-1,ChangeConfiguration(StableRaftConfiguration(Set(raft-member-1,
        // raft-member-2, raft-member-3, raft-member-4)))),3845)
        3845,
        // UniqueMsgEvent(MsgEvent(raft-member-4,raft-member-3,VoteCandidate(Term(0))),3867)
        3867,
        // UniqueMsgSend(MsgSend(raft-member-2,raft-member-2,BeginElection),3852)
        3852))

    // Should have next:
        // MsgEvent(raft-member-2,raft-member-1,
        //  RequestVote(Term(2),Actor[akka://new-system-0/user/raft-member-2#469873442],Term(0),0))

    val reorderedReplay = RunnerUtils.replayExperiment(reordered,
      schedulerConfig, deserializer.get_actors, None)

    println("Resulting Replay:")
    reorderedReplay.events.foreach {
      case e => println(e)
    }

    // Keep the same prefix of deliveries
    var ids = Seq() ++ reorderedReplay.events.flatMap {
      case UniqueMsgEvent(m,id) => Some(id)
      case UniqueTimerDelivery(t,id) => Some(id)
      case _ => None
    } ++ Seq(
      // UniqueMsgSend(MsgSend(raft-member-2,raft-member-1,RequestVote(Term(1),
      //   Actor[akka://new-system-3/user/raft-member-2#-919221483],Term(0),0)),3907)
      // (NEW! -- different Term)
      3907
    )

    val reordered2 = RunnerUtils.reorderDeliveries(reorderedReplay, ids)

    val reorderedReplay2 = RunnerUtils.replayExperiment(reordered2,
      schedulerConfig, deserializer.get_actors, None)

    println("Resulting Replay2:")
    reorderedReplay2.events.foreach {
      case e => println(e)
    }

    // Keep the same prefix of deliveries
    ids = Seq() ++ reorderedReplay2.events.flatMap {
      case UniqueMsgEvent(m,id) => Some(id)
      case UniqueTimerDelivery(t,id) => Some(id)
      case _ => None
    } ++ Seq(
      // UniqueMsgSend(MsgSend(Timer,raft-member-2,Timer(election-timer,ElectionTimeout,false,3)),3918)
      // (Second election timer. MOVED from earlier to here.)
      3918,
      // UniqueMsgSend(MsgSend(raft-member-1,raft-member-2,VoteCandidate(Term(0))),3937)
      3937
    )

    val reordered3 = RunnerUtils.reorderDeliveries(reorderedReplay2, ids)

    val reorderedReplay3 = RunnerUtils.replayExperiment(reordered3,
      schedulerConfig, deserializer.get_actors, None)

    println("Resulting Replay3:")
    reorderedReplay3.events.foreach {
      case e => println(e)
    }

    // Should have next:

    // BeginElection -> 2

    // Keep the same prefix of deliveries
    ids = Seq() ++ reorderedReplay3.events.flatMap {
      case UniqueMsgEvent(m,id) => Some(id)
      case UniqueTimerDelivery(t,id) => Some(id)
      case _ => None
    } ++ Seq(
      // UniqueMsgSend(MsgSend(raft-member-2,raft-member-2,BeginElection),3966)
      3966
    )

    val reordered4 = RunnerUtils.reorderDeliveries(reorderedReplay3, ids)

    val reorderedReplay4 = RunnerUtils.replayExperiment(reordered4,
      schedulerConfig, deserializer.get_actors, None)

    println("Resulting Replay4:")
    reorderedReplay4.events.foreach {
      case e => println(e)
    }

    // Should have next:

    // RequestVote 2 -> 1

    // Keep the same prefix of deliveries
    ids = Seq() ++ reorderedReplay4.events.flatMap {
      case UniqueMsgEvent(m,id) => Some(id)
      case UniqueTimerDelivery(t,id) => Some(id)
      case _ => None
    } ++ Seq(
      // UniqueMsgSend(MsgSend(raft-member-2,raft-member-1,RequestVote(Term(2),
      //  Actor[akka://new-system-12/user/raft-member-2#1273486631],Term(0),0)),3996)
      3996
    )

    val reordered5 = RunnerUtils.reorderDeliveries(reorderedReplay4, ids)

    val reorderedReplay5 = RunnerUtils.replayExperiment(reordered5,
      schedulerConfig, deserializer.get_actors, None)

    println("Resulting Replay5:")
    reorderedReplay5.events.foreach {
      case e => println(e)
    }

    // Should have next:

    // VoteCandidate 1 -> 2

    // Keep the same prefix of deliveries
    ids = Seq() ++ reorderedReplay5.events.flatMap {
      case UniqueMsgEvent(m,id) => Some(id)
      case UniqueTimerDelivery(t,id) => Some(id)
      case _ => None
    } ++ Seq(
      // UniqueMsgSend(MsgSend(raft-member-1,raft-member-2,VoteCandidate(Term(0))),4031)
      4031
    )

    val reordered6 = RunnerUtils.reorderDeliveries(reorderedReplay5, ids)

    val reorderedReplay6 = RunnerUtils.replayExperiment(reordered6,
      schedulerConfig, deserializer.get_actors, None)

    println("Final deliveries")
    RunnerUtils.printDeliveries(reorderedReplay6)

    val serializer = new ExperimentSerializer(
      fingerprintFactory,
      new RaftMessageSerializer)

    serializer.recordHandCraftedTrace(dir, reorderedReplay6)
  }
}
