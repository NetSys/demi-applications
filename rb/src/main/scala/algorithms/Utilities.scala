package akka.dispatch.verification.algorithm 

import akka.dispatch.verification._
import scala.collection.mutable.{HashMap, Queue}

// Utilities to use when looking at schedules etc.
object Utilities {
  // Check if an external event corresponds to a given internal event
  def extIsInt(ext: ExternalEvent, internal: Event) : Boolean = {
      ext match {
        case Start(_, name) =>
          internal match {
            case SpawnEvent(_, _, name, _) =>
              true
            case _ =>
              false
          }
        case Kill(name) =>
          internal match {
            case KillEvent(name) =>
              true
            case _ =>
              false
          }
        case Send(name, msg) =>
          internal match {
            case MsgSend("deadLetters", name, msg) =>
              true
            case _ =>
              false
          }
        case WaitQuiescence =>
          internal match {
            case Quiescence =>
              true
            case _ =>
              false
          }
        case Partition(a, b) =>
          internal match {
            case PartitionEvent((a, b)) =>
              true
            case _ =>
              false
          }
        case UnPartition(a, b) =>
          internal match {
            case UnPartitionEvent((a, b)) =>
              true
            case _ =>
              false
          }
      }
  }

  // Given an external trace and internal schedule, find the corresponding indices from
  // one to the other.
  def alignSchedules(trace: Array[ExternalEvent],
                                   sched: Queue[Event]) : HashMap[Int, Int] = {
    val eventTime = new HashMap[Int, Int]
    var idx = 0
    var externEvent = 0

    // Align schedule to external events
    while (idx < sched.length && 
            externEvent < trace.length)  {

      if (extIsInt(trace(externEvent), 
                   sched(idx))) {
        eventTime(externEvent) = idx
        externEvent += 1
      }
      idx += 1
    }
    eventTime
  }
}
