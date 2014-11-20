package akka.dispatch.verification.algorithm 

import akka.dispatch.verification._
import scala.collection.mutable.{HashSet, Stack, Queue, MutableList, HashMap}

object DependencyGraph {
  // Analyze a trace to figure out causal links. Quiescent states are currently
  // treated as sources and sinks.
  def analyzeTrace (eventTrace: Queue[Event]) {
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
          if (currentContext != "scheduler" || list.size > 0 || lastMsg == Quiescence) {
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
    println("============================================================================")
    for(((time, actor, msg), queue) <- enabledByStep) {
      if (actor != "scheduler" || list.size > 0 || lastMsg == Quiescence) {
        println(time + " " + actor + " ↢ " + msg + "  [" + causalLinks(time) + "]")
        for (en <- queue) {
          println("\t" + en)
        }
      }
    }
    println("============================================================================")
  }

}
