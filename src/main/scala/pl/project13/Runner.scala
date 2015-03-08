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

class RaftMessageFingerprinter extends MessageFingerprinter {
  // TODO(cs): might be an easier way to do this. See ActorRef API.
  val refRegex = ".*raft-member-(\\d+).*".r
  val systemRegex = ".*(new-system-\\d+).*".r

  override def fingerprint(msg: Any) : MessageFingerprint = {
    val superResult = super.fingerprint(msg)
    if (superResult != null) {
      return superResult
    }

    def removeId(ref: ActorRef) : String = {
      ref.toString match {
        case refRegex(member) => return "raft-member-" + member
        case _ => return ref.toString
      }
    }
    val str = msg match {
      case RequestVote(term, ref, lastTerm, lastIdx) =>
        (("RequestVote", term, removeId(ref), lastTerm, lastIdx)).toString
      case LeaderIs(Some(ref), msg) =>
        ("LeaderIs", removeId(ref)).toString
      case m =>
        // Lazy person's approach: instead of dealing properly with all
        // message types, just do a regex on the string
        m.toString match {
          case systemRegex(system) => m.toString.replace(system, "")
          case _ => m.toString
        }
    }
    return BasicFingerprint(str)
  }
}

class ClientMessageGenerator(raft_members: Seq[String]) extends MessageGenerator {
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
    return Send(dst, () =>
      ClientMessage[AppendWord](Instrumenter().actorSystem.deadLetters, AppendWord(word)))
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

  val members = (1 to 9) map { i => s"raft-member-$i" }

  val prefix = Array[ExternalEvent]() ++
    members.map(member =>
      Start(Init.actorCtor, member)) ++
    members.map(member =>
      Send(member, Init.startCtor)) ++
    Array[ExternalEvent](
    WaitQuiescence(),
    WaitTimers(1),
    Continue(10)
    //WaitQuiescence
    // Continue(500)
  )

  val weights = new FuzzerWeights(kill=0.01, send=0.3, wait_quiescence=0.1,
                                  wait_timers=0.3, partition=0.1, unpartition=0.1,
                                  continue=0.3)
  val messageGen = new ClientMessageGenerator(members)
  val fuzzer = new Fuzzer(500, weights, messageGen, prefix)

  def shutdownCallback() = {
    raftChecks.clear
  }

  Instrumenter().registerShutdownCallback(shutdownCallback)

  val fuzz = false

  var traceFound: EventTrace = null
  var violationFound: ViolationFingerprint = null
  if (fuzz) {
    val replayer = new ReplayScheduler(new RaftMessageFingerprinter, false, false)
    replayer.setEventMapper(Init.eventMapper)
    val pair = RunnerUtils.fuzz(fuzzer, raftChecks.invariant,
                                validate_replay=Some(replayer))
    traceFound = pair._1
    violationFound = pair._2

    println("----------")
    println("trace:")
    for (e <- traceFound) {
      println(e)
    }
    println("----------")
  }

  val serializer = new ExperimentSerializer(
      new RaftMessageFingerprinter,
      new RaftMessageSerializer)

  val dir = if (fuzz) serializer.record_experiment("akka-raft-fuzz",
    traceFound.filterCheckpointMessages(), violationFound) else
    "/Users/cs/Research/UCB/code/sts2-applications/experiments/akka-raft-fuzz_2015_03_07_16_28_29"

  /*
  println("Trying randomDDMin")
  var (mcs1, stats1, mcs_execution1, violation1) =
    RunnerUtils.randomDDMin(dir,
      new RaftMessageFingerprinter,
      new RaftMessageDeserializer(Instrumenter().actorSystem),
      raftChecks.invariant)

  serializer.serializeMCS(dir, mcs1, stats1, mcs_execution1, violation1)
  */

  println("Trying STSSchedDDMinNoPeak")
  // Dissallow peek:
  var (mcs2, stats2, mcs_execution2, violation2) =
    RunnerUtils.stsSchedDDMin(dir,
      new RaftMessageFingerprinter,
      new RaftMessageDeserializer(Instrumenter().actorSystem),
      false,
      raftChecks.invariant,
      Some(Init.eventMapper))

  serializer.serializeMCS(dir, mcs2, stats2, mcs_execution2, violation2)

  println("Trying STSSchedDDMin")
  // Allow peek:
  var (mcs3, stats3, mcs_execution3, violation3) =
    RunnerUtils.stsSchedDDMin(dir,
      new RaftMessageFingerprinter,
      new RaftMessageDeserializer(Instrumenter().actorSystem),
      true,
      raftChecks.invariant,
      Some(Init.eventMapper))

  serializer.serializeMCS(dir, mcs3, stats3, mcs_execution3, violation3)

  println("Trying RoundRobinDDMin")
  var (mcs4, stats4, mcs_execution4, violation4) =
    RunnerUtils.roundRobinDDMin(dir,
      new RaftMessageFingerprinter,
      new RaftMessageDeserializer(Instrumenter().actorSystem),
      raftChecks.invariant)

  serializer.serializeMCS(dir, mcs4, stats4, mcs_execution4, violation4)
}
