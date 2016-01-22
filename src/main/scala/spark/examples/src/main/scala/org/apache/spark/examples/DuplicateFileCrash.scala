
package org.apache.spark.examples

import akka.dispatch.verification._
import org.apache.spark.deploy.master.FileSystemPersistenceEngine

case class DuplicateFileCrash() extends ViolationFingerprint {
  def matches(other: ViolationFingerprint) : Boolean = {
    if (!other.isInstanceOf[DuplicateFileCrash]) {
      return false
    }
    return true
  }
  def affectedNodes(): Seq[String] = Seq("Master")
}

object DuplicateFileCrash {
  def invariant(s: Seq[akka.dispatch.verification.ExternalEvent],
                c: scala.collection.mutable.HashMap[String,Option[akka.dispatch.verification.CheckpointReply]])
              : Option[akka.dispatch.verification.ViolationFingerprint] = {
    if (FileSystemPersistenceEngine.hasThrownException.get()) {
      return Some(DuplicateFileCrash())
    }
    return None
  }
}
