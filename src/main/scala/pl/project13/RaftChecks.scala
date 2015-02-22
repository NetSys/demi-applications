package runner.raftchecks

import akka.actor.{ Actor, ActorRef, DeadLetter }
import akka.actor.ActorSystem
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
// + A simple one:
// Liveness: no node should crash.

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

  def serialize() = {
    //(new Tuple1(fingerprints)).pickleTo(file)
  }
}

class RaftChecks {
  // -- Checkers --
  val electionSafety = new ElectionSafetyChecker(this)
  val logMatch = new LogMatchChecker(this)
  val leaderCompleteness = new LeaderCompletenessChecker(this)
  val stateMachine = new StateMachineChecker(this)

  // -- State --
  var term2leader = new HashMap[Term, String]
  // Only contains the most recent ReplicatedLogs.
  // TODO(cs): could potentially find more violations if we kept a history of
  // all replicated logs.
  val actor2log = new HashMap[String, ReplicatedLog[Cmnd]]
  val actor2AllEntries = new HashMap[String, HashSet[(Cmnd, Term, Int)]]
  val allCommitted = new HashSet[(Cmnd, Term, Int)]

  def ingestCheckpoint(checkpoint: HashMap[String,CheckpointReply]) = {
    for ((actor, reply) <- checkpoint) {
      val data = reply.data.asInstanceOf[List[Any]]
      val replicatedLog = data(0).asInstanceOf[ReplicatedLog[Cmnd]]
      val nextIndex = data(1).asInstanceOf[LogIndexMap]
      val matchIndex = data(2).asInstanceOf[LogIndexMap]
      val state = data(3).asInstanceOf[Metadata]

      state match {
        case LeaderMeta(_, term, _) =>
          term2leader(term) = actor
        case _ => None
      }

      actor2log(actor) = replicatedLog

      for (entry <- replicatedLog.committedEntries) {
        allCommitted += ((entry.command, entry.term, entry.index))
      }

      if (!(actor2AllEntries contains actor)) {
        actor2AllEntries(actor) = new HashSet[(Cmnd, Term, Int)]
      }

      for (entry <- replicatedLog.entries) {
        actor2AllEntries(actor) += ((entry.command, entry.term, entry.index))
      }
    }
  }

  def check(checkpoint: HashMap[String,CheckpointReply]) : Option[HashSet[String]] = {
    ingestCheckpoint(checkpoint)

    var violations = new HashSet[String]()

    // Run checks that are specific to an individual actor
    for ((actor, reply) <- checkpoint) {
      val data = reply.data.asInstanceOf[List[Any]]
      checkActor(actor, data) match {
        case Some(fingerprints) =>
          violations ++= fingerprints
        case None => None
      }
    }

    // Run global checks
    leaderCompleteness.check() match {
      case Some(fingerprint) =>
        violations += fingerprint
      case None => None
    }

    stateMachine.check() match {
      case Some(fingerprint) =>
        violations += fingerprint
      case None => None
    }

    if (!violations.isEmpty) {
      return Some(violations)
    }
    return None
  }

  // Run checks that are specific to an individual actor
  // Pre: ingestCheckpoint was invoked prior to this.
  def checkActor(actor: String, data: List[Any]) : Option[Seq[String]] = {
    val metaData = data(3).asInstanceOf[Metadata]

    var violations = new ListBuffer[String]()

    electionSafety.checkActor(actor, metaData) match {
      case Some(fingerprint) =>
        violations += fingerprint
      case None => None
    }

    logMatch.checkActor(actor) match {
      case Some(fingerprint) =>
        violations += fingerprint
      case None => None
    }

    if (!violations.isEmpty) {
      return Some(violations)
    }
    return None
  }
}

// Election Safety: at most one leader can be elected in a given term. §5.2
class ElectionSafetyChecker(parent: RaftChecks) {
  // N.B. we keep our own private term2leader, rather than using parent's.
  // (Parent's assumes that ElectionSafety holds.)
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
class LogMatchChecker(parent: RaftChecks) {

  def checkActor(actor: String) : Option[String] = {
    val otherActorLogs = parent.actor2log.filter { case (a,_) => a != actor }
    val log = parent.actor2log(actor)
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

// Leader Completeness: if a log entry is committed in a given term, then that
//     entry will be present in the logs of the leaders for all higher-numbered
//     terms. §5.4
class LeaderCompletenessChecker(parent: RaftChecks) {

  def check() : Option[String] = {
    val sortedTerms = parent.term2leader.keys.toArray.sortWith((a,b) => a < b)
    if (sortedTerms.isEmpty) {
      return None
    }
    // The index of the first sortedTerm that is > than the current committed
    // entry.
    var termWatermark = 0
    val sortedCommitted = parent.allCommitted.toArray.sortWith((c1, c2) => c1._2 < c2._2)
    for (committed <- sortedCommitted) {
      // Move the waterMark if necessary
      while (termWatermark < sortedTerms.length &&
             sortedTerms(termWatermark) <= committed._2) {
        termWatermark += 1
      }
      if (termWatermark >= sortedTerms.length) {
        return None
      }
      for (termIdx <- (termWatermark until sortedTerms.length)) {
        val term = sortedTerms(termIdx)
        val leader = parent.term2leader(term)
        if (!(parent.actor2AllEntries(leader) contains committed)) {
          return Some("LeaderCompleteness:"+leader+":"+committed)
        }
      }
    }
    return None
  }
}

// StateMachine Safety: if a server has applied a log entry at a given index to
//     its state machine, no other server will ever apply a different log entry for
//     the same index. §5.4.3
class StateMachineChecker(parent: RaftChecks) {
  // TODO(cs): for now, we just check committed entries. To get a perfect view
  // of applied entries, we'd need the RaftActors to send us a message every
  // time they apply an entry (which, AFAICT, happens immediately after they infer
  // infer that they should commit an given entry)
  def check() : Option[String] = {
    val allCommittedIndices = parent.allCommitted.toArray.map(c => c._3)
    if (parent.allCommitted.size != allCommittedIndices.size) {
      val counts = new MultiSet[Int]
      counts ++= allCommittedIndices
      val duplicates = counts.m.filter({case (k, v) => v.length > 1}).keys
      return Some("StateMachine:"+duplicates.toString)
    }
    return None
  }
}

