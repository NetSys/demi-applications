
package org.apache.spark.examples

import akka.dispatch.verification._
import org.apache.spark.deploy.master.Master

class CrashUponRecovery() extends ViolationFingerprint {
  def matches(other: ViolationFingerprint) : Boolean = {
    if (!other.isInstanceOf[CrashUponRecovery]) {
      return false
    }
    return true
  }
  def affectedNodes(): Seq[String] = Seq.empty
}

object CrashUponRecovery {
  def invariant(s: Seq[akka.dispatch.verification.ExternalEvent],
                c: scala.collection.mutable.HashMap[String,Option[akka.dispatch.verification.CheckpointReply]])
              : Option[akka.dispatch.verification.ViolationFingerprint] = {
    if (Master.hasCausedNPE) {
      return Some(new CrashUponRecovery)
    }
    return None
  }
}
