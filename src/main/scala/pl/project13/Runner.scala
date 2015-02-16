import akka.actor.{ Actor, ActorRef, DeadLetter }
import akka.actor.ActorSystem
import akka.actor.Props
import akka.dispatch.verification._
import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.Map
import scala.util.Random
import pl.project13.scala.akka.raft.example._
import pl.project13.scala.akka.raft.protocol._
import pl.project13.scala.akka.raft.example.protocol._
import pl.project13.scala.akka.raft._
import pl.project13.scala.akka.raft.model._

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

case class RaftViolation(fingerprints: HashSet[String]) extends ViolationFingerprint {
  def matches(other: ViolationFingerprint) : Boolean = {
    other match {
      case RaftViolation(otherFingerprint) =>
        // Slack matching algorithm for now: if any fingerprint matches, the whole
        // thing matches
        return !fingerprints.intersect(otherFingerprint).isEmpty
      case _ => return false
    }
  }
}

// Election Safety: at most one leader can be elected in a given term. §5.2
class ElectionSafetyChecker {
  var term2leader = new HashMap[Term, String]

  def checkActor(actor: String, state: Metadata) : Option[String] = {
    state match {
      case LeaderMeta(_, term, _) =>
        if ((term2leader contains term) && term2leader(term) != actor) {
          return Some("ElectionSafety:" + actor + ":" + term)
        }
        term2leader(term) = actor
        return None
      case _ =>
        return None
    }
  }
}

// LogMatching: if two logs contain an entry with the same index and term, then
//     the logs are identical in all entries up through the given index. §5.3
class LogMatchChecker {
  // Only contains the most recent ReplicatedLogs.
  // TODO(cs): could potentially find more violations if we kept a history of
  // all replicated logs.
  val actor2log = new HashMap[String, ReplicatedLog[Cmnd]]

  def checkActor(actor: String, log: ReplicatedLog[Cmnd]) : Option[String] = {
    actor2log(actor) = log
    val otherActorLogs = actor2log.filter { case (a,_) => a != actor }
    for (otherActorLog <- otherActorLogs) {
      val otherActor = otherActorLog._1
      val otherLog = otherActorLog._2
      // First, find the last entry in both logs that has the same index and
      // term.
      val otherIdxTerms = otherLog.entries.map(entry => (entry.index, entry.term)).toSet
      val reversed = log.entries.reverse
      val reverseMatchIdx = reversed.indexWhere(
          entry => (otherIdxTerms contains (entry.index, entry.term)))
      if (reverseMatchIdx != -1) {
        // Now verify that the logs are identical up to the match.
        val matchIdx = (log.length - 1) - reverseMatchIdx
        if (otherLog.length < matchIdx) {
          return Some("LogMatch:"+actor+":"+otherActor+":"+matchIdx)
        }
        var currentIdx = 0
        while (currentIdx <= matchIdx) {
          val myEntry = log(currentIdx)
          val otherEntry = otherLog(currentIdx)
          if (myEntry.command != otherEntry.command ||
              myEntry.term != otherEntry.term ||
              myEntry.index != otherEntry.index) {
            return Some("LogMatch:"+actor+":"+otherActor+":"+currentIdx)
          }
          currentIdx += 1
        }
      }
    }
    return None
  }
}

object Main extends App {
  // Correctness properties of Uniform Consensus:
  // ------------
  // Termination: Every correct process eventually decides some value.
  // Validity: If a process decides v, then v was proposed by some process.
  // Integrity: No process decides twice.
  // Uniform agreement: No two processes decide differently.
  // ------------
  // Correctness properties of Epoch Change:
  // ------------
  // Monotonicity: If a correct process starts an epoch (ts, l) and later
  //               starts an epoch (ts′, l′), then ts′ > ts.
  // Consistency: If a correct process starts an epoch (ts, l) and another
  //              correct process starts an epoch (ts′, l′) with ts = ts′, then l = l′.
  // Eventual leadership: There is a time after which every correct process
  //             has started some epoch and starts no further epoch, such that the last epoch
  //             started at every correct process is epoch (ts, l) and process l is correct.
  // ------------
  // Correctness properties of Epoch Consensus:
  // ------------
  // Validity: If a correct process ep-decides v, then v was ep-proposed
  //    by the leader l′ of some epoch consensus with timestamp ts′ ≤ ts and leader l′.
  // Uniform agreement: No two processes ep-decide differently.
  // Integrity: Every correct process ep-decides at most once.
  // Lock-in: If a correct process has ep-decided v in an epoch consensus
  //    with timestamp ts′ < ts, then no correct process ep-decides a value
  //    different from v.
  // Termination: If the leader l is correct, has ep-proposed a value, and
  //   no correct process aborts this epoch consensus, then every correct process
  //   eventually ep-decides some value.
  // Abort behavior: When a correct process aborts an epoch consensus, it
  //   eventu- ally will have completed the abort; moreover, a correct process
  //   completes an abort only if the epoch consensus has been aborted by some
  //   correct process.
  // -----------
  // The safety conditions according to the Raft paper are:
  // -----------
  // Election Safety: at most one leader can be elected in a given term. §5.2
  // Leader Append-Only: a leader never overwrites or deletes entries in its log;
  //     it only appends new entries. §5.3
  // LogMatching: if two logs contain an entry with the same index and term, then
  //     the logs are identical in all entries up through the given index. §5.3
  // Leader Completeness: if a log entry is committed in a given term, then that
  //     entry will be present in the logs of the leaders for all higher-numbered
  //     terms. §5.4
  // StateMachine Safety: if a server has applied a log entry at a given index to
  //     its state machine, no other server will ever apply a different log entry for
  //     the same index. §5.4.3
  // -------------
  // TODO(cs): A simple one: no node should crash.

  val electionSafety = new ElectionSafetyChecker
  val logMatchChecker = new LogMatchChecker

  def invariant(seq: Seq[ExternalEvent], checkpoint: HashMap[String,CheckpointReply]) : Option[ViolationFingerprint] = {
    var violations = new HashSet[String]()
    for ((actor, reply) <- checkpoint) {
      val list = reply.data.asInstanceOf[List[Any]]
      val replicatedLog = list(0).asInstanceOf[ReplicatedLog[Cmnd]]
      val nextIndex = list(1).asInstanceOf[LogIndexMap]
      val matchIndex = list(2).asInstanceOf[LogIndexMap]
      val metaData = list(3).asInstanceOf[Metadata]

      electionSafety.checkActor(actor, metaData) match {
        case Some(fingerprint) =>
          violations += fingerprint
        case None => None
      }

      logMatchChecker.checkActor(actor, replicatedLog) match {
        case Some(fingerprint) =>
          violations += fingerprint
        case None => None
      }
    }

    if (!violations.isEmpty) {
      return Some(RaftViolation(violations))
    }
    return None
  }

  val members = (1 to 9) map { i => s"raft-member-$i" }

  val prefix = Array[ExternalEvent]() ++
    //Array[ExternalEvent](Start(() =>
    //  RaftClientActor.props(Instrumenter().actorSystem() / "raft-member-*"), "client")) ++
    members.map(member =>
      Start(() => Props.create(classOf[WordConcatRaftActor]), member)) ++
    members.map(member =>
      Send(member, () => {
        val clusterRefs = Instrumenter().actorMappings.filter({
            case (k,v) => k != "client" && !ActorTypes.systemActor(k)
        }).values
        ChangeConfiguration(ClusterConfiguration(clusterRefs))
      })) ++
    Array[ExternalEvent](
    WaitQuiescence,
    WaitTimers(1)
    //WaitQuiescence
    // Continue(500)
  )

  val weights = new FuzzerWeights(0.01, 0.3, 0.3, 0.0, 0.05, 0.05, 0.2)
  val messageGen = new ClientMessageGenerator(members)
  val fuzzer = new Fuzzer(30, weights, messageGen, prefix)
  val fuzzTest = fuzzer.generateFuzzTest()
  println(fuzzTest)

  val sched = new RandomScheduler(1, false, 30)
  sched.setInvariant(invariant)
  Instrumenter().scheduler = sched
  sched.explore(fuzzTest)
  println("Returned to main with events")
  sched.shutdown()
  println("shutdown successfully")
}
