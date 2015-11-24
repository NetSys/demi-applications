package akka.dispatch.verification

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

case class DDViolation(val fingerprint2affectedNodes: Map[String, Set[String]])
           extends ViolationFingerprint {
  def matches(other: ViolationFingerprint) : Boolean = {
    other match {
      case DDViolation(otherMap) =>
        // Slack matching algorithm for now: if any fingerprint matches, the whole
        // thing matches
        return !fingerprint2affectedNodes.keys.toSet.intersect(otherMap.keys.toSet).isEmpty
      case _ => return false
    }
  }

  def affectedNodes() : Seq[String] = {
    return fingerprint2affectedNodes.values.toSeq.flatten
  }
}

class DDChecks {
  def invariant(seq: Seq[ExternalEvent], checkpoint: HashMap[String,Option[CheckpointReply]]) : Option[ViolationFingerprint] = {
    return None
  }

  def clear() {}
}

