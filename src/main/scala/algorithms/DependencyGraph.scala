package akka.dispatch.verification.algorithm 

import akka.dispatch.verification._
import scala.collection.mutable.{HashSet, Stack, Queue, MutableList, HashMap}
import scala.util.control.Breaks._

case class CurrentDispatch (time: Int, actor: String, msg: Any)

case class IdxedEvent (schedIdx: Int, event: Event) 
case class LocatedIdx (schedIdx: Int, time: Int)
case class TraceAnalysisResult (trace: Array[Event],
                                traceIdxToTime: HashMap[Int, Int], // Time when a trace event is processed
                                executedAtStep: Queue[(CurrentDispatch,  List[IdxedEvent])],
                                eventLinks: HashMap[Int, Queue[LocatedIdx]])

object DependencyGraph {
  private[this] def contextPredicate (currentContext: String, 
                                      list: List[IdxedEvent], 
                                      lastMsg: Any) : Boolean = {
    (currentContext != "scheduler") || 
      (list.size > 0) || 
      lastMsg == Quiescence
  }

  // Given an old trace and schedule that leads to a bug, create a new one that is similar to the original
  def recreateSchedule (origTrace : Array[ExternalEvent],
                        removeFromOrig: Array[Int],
                        origSched: Queue[Event],
                        peekedSched: Queue[Event]) : Queue[Event] = {
    val shortSched = new Queue[Event]
    val origTraceAnalysis = analyzeTrace(origSched)
    val origReverse = reverseLinks(origTraceAnalysis)
    val origAligned = Utilities.alignSchedules(origTrace, origSched)
    val removeIndices = new HashSet[Int]
    val toVisit = new HashSet[Int]
    val visited = new HashSet[Int]
    for (e <- removeFromOrig) {
      val idx = origAligned(e)
      removeIndices += idx 
      toVisit += idx
      while (!toVisit.isEmpty) {
        val nidx = toVisit.head
        visited += nidx
        // Time starts at 1
        if (origReverse(nidx).length > 0 && origReverse(nidx).length <= 2) {
          val ev = origTraceAnalysis.executedAtStep(origReverse(nidx).head - 1)
          ev match {
            case (CurrentDispatch(time, actor, msg), idxEvent) =>
              for (IdxedEvent(s, t) <- idxEvent) {
                if (!(visited contains s)) {
                  removeIndices += s
                  toVisit += s
                }
              }
              toVisit -= nidx
          }
        } else {
          toVisit -= nidx
        }
      }
    }
    // Analysis for the peeked schedule
    val peekAnalysis = analyzeTrace(peekedSched)
    val peekReverse = reverseLinks(peekAnalysis)
    val keepIndices = (0 until origTrace.length).filter(idx => !(removeFromOrig contains idx))
    // New external trace after things have been removed 
    val newExt = keepIndices.map(origTrace(_)).toArray
    val newAligned = Utilities.alignSchedules(newExt, peekedSched)

    // Reproduce the original schedule without the missing elements. This is not enough in reality we want to meld the 
    // new one the appropriate way, but is a good start.
    for (eidx <- 0 until origSched.length) {
      if (!(removeIndices contains eidx)) {
        shortSched += origTraceAnalysis.trace(eidx)
      }
    }
    shortSched
  }

  // Given an analysis result, compute links connecting a particular event in the schedule
  // to the time when it is processed/executed. (This of course only holds for sends;
  // receives, spawns and others only affect the instantaneous timestep).
  def reverseLinks (analysis: TraceAnalysisResult) : HashMap[Int, Queue[Int]] = {
    val reverseLinks = new HashMap[Int, Queue[Int]]
    for ((CurrentDispatch(time, _, _), events) <- analysis.executedAtStep) {
      for (IdxedEvent(idx, ev) <- events) {
        reverseLinks(idx) = new Queue[Int]
        ev match {
          case _ : MsgSend => ()
          case _ =>
            reverseLinks(idx) += time 
        }
      }
      for (LocatedIdx(idx, t) <- analysis.eventLinks(time)) {
        // We use -1 to record the start of replay
        if (idx >= 0) { 
          reverseLinks(idx) += time 
        }
      }
    }
    reverseLinks
  }

  // Analyze a trace to figure out the set of events executeded at each step and the step at which a particular step was
  // enabled. Quiescent states are currently treated as sources and sinks.
  def analyzeTrace (eventTrace: Queue[Event]) : TraceAnalysisResult =  {
    var currentContext = "scheduler"
    var currentStep = 1

    val executedAtStep = new Queue[(CurrentDispatch,  List[IdxedEvent])]
    var lastRecv : IdxedEvent = null
    var lastMsg : Any = null

    var lastQuiescent = LocatedIdx(-1, 0)
    var list = new MutableList[IdxedEvent]

    val traceIdxToTime = new HashMap[Int, Int]
    val eventLinks = new HashMap[Int, Queue[LocatedIdx]]
    val messageSendTime = new HashMap[(String, String, Any), LocatedIdx]
    val contextsSinceQuiescence = new Queue[LocatedIdx]
    val traceAsArray = eventTrace.toArray

    eventLinks(currentStep) = new Queue[LocatedIdx]

    for (evIdx <- 0 until traceAsArray.length) {
      val ev = traceAsArray(evIdx)
      ev match {
        case s: SpawnEvent =>
          list += IdxedEvent(evIdx, s)
          traceIdxToTime(evIdx) = currentStep
        case m: MsgSend =>
          messageSendTime((m.sender, m.receiver, m.msg)) = LocatedIdx(evIdx, currentStep)
          list += IdxedEvent(evIdx, m)
          traceIdxToTime(evIdx) = currentStep
        case ChangeContext(actor) =>
          if (contextPredicate(currentContext, list.toList, lastMsg)) {
            if (currentContext == "scheduler" && lastMsg != Quiescence) {
              // Everything the scheduler does must causally happen after the last quiescent
              // period
              eventLinks(currentStep) += lastQuiescent
            }
            contextsSinceQuiescence += LocatedIdx(evIdx, currentStep)
            executedAtStep += ((CurrentDispatch(currentStep, currentContext, lastMsg), list.toList))
            currentStep += 1
          }
          traceIdxToTime(evIdx) = currentStep
          list.clear()
          currentContext = actor
          lastMsg = null
          eventLinks(currentStep) = new Queue[LocatedIdx]
          if (lastRecv != null) {
            val lastRec = lastRecv.event.asInstanceOf[MsgEvent]
            require(lastRec.receiver == currentContext)
            traceIdxToTime(lastRecv.schedIdx) = currentStep
            lastMsg = lastRec.msg
            eventLinks(currentStep) += 
                messageSendTime((lastRec.sender, lastRec.receiver, lastRec.msg))
            list += lastRecv // Add the recv to the schedule
            lastRecv = null
          }
          list += IdxedEvent(evIdx, ev)
        case e: MsgEvent =>
          lastRecv = IdxedEvent(evIdx, e)
        case Quiescence =>
          lastMsg = Quiescence
          lastQuiescent = LocatedIdx(evIdx, currentStep)
          eventLinks(currentStep) ++= contextsSinceQuiescence
          traceIdxToTime(evIdx) = currentStep
          contextsSinceQuiescence.clear()
          list += IdxedEvent(evIdx, ev)
        case _ =>
          traceIdxToTime(evIdx) = currentStep
          list += IdxedEvent(evIdx, ev)
      }
    }
    executedAtStep += ((CurrentDispatch(currentStep, currentContext, lastMsg), list.toList))
    TraceAnalysisResult(traceAsArray, traceIdxToTime, executedAtStep, eventLinks)
  }
}
