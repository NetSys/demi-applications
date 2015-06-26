
package org.apache.spark.examples

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.TaskLocality
import org.apache.spark.scheduler.local._
import akka.dispatch.verification._


// TODO(cs): probably want to tweak this fingerprint a bit.
class LocalityInversion(val taskSetId: String) extends ViolationFingerprint {
  def matches(other: ViolationFingerprint) : Boolean = {
    if (!other.isInstanceOf[LocalityInversion]) {
      return false
    }
    return other.asInstanceOf[LocalityInversion].taskSetId == taskSetId
  }
  def affectedNodes(): Seq[String] = Seq.empty
}

object LocalityInversion {
  def invariant(s: Seq[akka.dispatch.verification.ExternalEvent],
                c: scala.collection.mutable.HashMap[String,Option[akka.dispatch.verification.CheckpointReply]])
              : Option[akka.dispatch.verification.ViolationFingerprint] = {
    while (!TaskSetManager.decisions.isEmpty) {
      val (taskSet,
           executor,
           index,
           pendingTasksForExecutor,
           pendingTasksForHost,
           pendingTasksForRack,
           pendingTasksWithNoPrefs,
           allPendingTasks) = TaskSetManager.decisions.dequeue
      // First find out the lowest locality level for the chosen task
      def localityLevel() : TaskLocality.TaskLocality = {
        for ((level, map) <- Seq(
          (TaskLocality.PROCESS_LOCAL, pendingTasksForExecutor),
          (TaskLocality.NODE_LOCAL, pendingTasksForHost),
          (TaskLocality.RACK_LOCAL, pendingTasksForRack))) {
          for (array <- map.values) {
            array.find(idx => idx == index) match {
              case Some(idx) => return level
              case None =>
            }
          }
        }
        // TODO(cs): check if TaskLocality.NO_PREF is in the correct
        // index in TaskLocality.scala
        for ((level, array) <- Seq(
          (TaskLocality.NO_PREF, pendingTasksWithNoPrefs),
          (TaskLocality.ANY, allPendingTasks))) {
          array.find(idx => idx == index) match {
            case Some(idx) => return level
            case None =>
          }
        }
        // Else it's speculative, which we currently assume isn't turned on.
        throw new IllegalStateException("No locality level?")
      }

      val chosenLevel = localityLevel()
      // Now see if there were any pending tasks that were more local than
      // the one chosen.
      def lowestPendingLocality() : TaskLocality.TaskLocality = {
        for ((level, map) <- Seq(
          (TaskLocality.PROCESS_LOCAL, pendingTasksForExecutor),
          (TaskLocality.NODE_LOCAL, pendingTasksForHost),
          (TaskLocality.RACK_LOCAL, pendingTasksForRack))) {
          for (array <- map.values) {
            if (!array.isEmpty) {
              return level
            }
          }
        }

        for ((level, array) <- Seq(
          (TaskLocality.NO_PREF, pendingTasksWithNoPrefs),
          (TaskLocality.ANY, allPendingTasks))) {
          if (!array.isEmpty) {
            return level
          }
        }

        // Else it's speculative, which we currently assume isn't turned on.
        throw new IllegalStateException("No locality level?")
      }

      val lowestLevel = lowestPendingLocality()
      println("lowestLevel:"+lowestLevel+" chosenLevel:"+chosenLevel)
      if (lowestLevel < chosenLevel) {
        return Some(new LocalityInversion(taskSet))
      }
    }
    return None
  }
}
