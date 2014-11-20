package akka.dispatch.verification.algorithm 

import akka.dispatch.verification._
import scala.collection.mutable.{HashSet, Stack, Queue, MutableList, HashMap}
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
