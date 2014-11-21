package akka.dispatch.verification.algorithm 

import akka.dispatch.verification._
import scala.collection.mutable.{HashSet, Stack, Queue, MutableList, HashMap}
import scala.util.control.Breaks._

case class TraceAnalysisResult (enabledEvents: Queue[((Int, String, Any),  List[Event])],
                                eventLinks: HashMap[Int, Queue[Int]])

object DependencyGraph {
  private[this] def contextPredicate (currentContext: String, 
                                      list: List[Event], 
                                      lastMsg: Any) : Boolean = {
    currentContext != "scheduler" || 
      list.size > 0 || 
      lastMsg == Quiescence
  }

  private[this] def extIsInt(ext: ExternalEvent, internal: Event) : Boolean = {
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

  private[this] def alignSchedules(trace: Array[ExternalEvent],
                                   sched: Queue[Event]) : HashMap[Int, Int] = {
    val eventTime = new HashMap[Int, Int]
    var idx = 0
    var externEvent = 0

    // Align schedule to external events
    while (idx < sched.length && 
            externEvent < trace.length)  {

      if (extIsInt(trace(externEvent), 
                   sched(idx))) {
        // Time is 1 indexed not 0 indexed
        eventTime(externEvent) = idx + 1
        externEvent += 1
      }
      idx += 1
    }
    eventTime
  }

  // Given an old trace and schedule that leads to a bug, create a new one that is similar to the original
  def recreateSchedule (origTrace : Array[ExternalEvent],
                        removeFromOrig: Array[Int],
                        origSched: Queue[Event],
                        peekedSched: Queue[Event]) : Queue[Event] = {
    val shortSched = new Queue[Event]
    val origTraceAnalysis = analyzeTrace(origSched)
    val origTraceReverse = reverseLinks(origTraceAnalysis)
    val origAligned = alignSchedules(origTrace, origSched)
    val fixedOrig = new Queue[Event]
    val removedTimes = new HashSet[Int]()
    for (ext <- removeFromOrig) {
      removedTimes += origAligned(ext)
    }
    shortSched
  }

  // Given an analysis result (which has links in the forward direction, compute links in the reverse direction).
  def reverseLinks (analysis: TraceAnalysisResult) : HashMap[Int, Queue[Int]] = {
    val reverseLinks = new HashMap[Int, Queue[Int]]
    reverseLinks(0) = new Queue[Int]
    for (((time, _, _), _) <- analysis.enabledEvents) {
      reverseLinks(time) = new Queue[Int]
      for (l <- analysis.eventLinks(time)) {
        reverseLinks(l) += time
      }
    }
    reverseLinks
  }

  // Analyze a trace to figure out the set of events enabled at each step and the step at which a particular step was
  // enabled. Quiescent states are currently treated as sources and sinks.
  def analyzeTrace (eventTrace: Queue[Event]) : TraceAnalysisResult =  {
    var currentContext = "scheduler"
    var currentStep = 1
    val enabledByStep = new Queue[((Int, String, Any),  List[Event])]
    var lastRecv : MsgEvent = null
    var lastMsg : Any = null
    var lastQuiescent = 0
    var list = new MutableList[Event]
    val causalLinks = new HashMap[Int, Queue[Int]]
    val messageSendTime = new HashMap[(String, String, Any), Int]
    val eventsSinceQuiescence = new Queue[Int]
    causalLinks(currentStep) = new Queue[Int]
    for (ev <- eventTrace) {
      ev match {
        case s: SpawnEvent =>
          list += s
        case m: MsgSend =>
          messageSendTime((m.sender, m.receiver, m.msg)) = currentStep
          list += m
        case ChangeContext(actor) =>
          if (contextPredicate(currentContext, list.toList, lastMsg)) {
            if (currentContext == "scheduler" && lastMsg != Quiescence) {
              // Everything the scheduler does must causally happen after the last quiescent
              // period
              causalLinks(currentStep) += lastQuiescent
            }
            eventsSinceQuiescence += currentStep
            enabledByStep += (((currentStep, currentContext, lastMsg), list.toList))
            currentStep += 1
          }
          list.clear()
          currentContext = actor
          lastMsg = null
          causalLinks(currentStep) = new Queue[Int]
          if (lastRecv != null) {
            require(lastRecv.receiver == currentContext)
            lastMsg = lastRecv.msg
            causalLinks(currentStep) += 
                messageSendTime((lastRecv.sender, lastRecv.receiver, lastRecv.msg))
            lastRecv = null
          }
        case e: MsgEvent =>
          lastRecv = e

        case Quiescence =>
          lastMsg = Quiescence
          lastQuiescent = currentStep
          causalLinks(currentStep) ++= eventsSinceQuiescence
          eventsSinceQuiescence.clear()
        case _ =>
          ()
      }
    }
    enabledByStep += (((currentStep, currentContext, lastMsg), list.toList))
    TraceAnalysisResult(enabledByStep, causalLinks)
  }
}
