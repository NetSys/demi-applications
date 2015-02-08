import broadcast.{ BroadcastNode, RBBroadcast, DataMessage }
import akka.actor.{ Actor, ActorRef, DeadLetter }
import akka.actor.ActorSystem
import akka.actor.Props
import akka.dispatch.verification._
import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map

object Main extends App {
  val actors = Array("bcast0",
                     "bcast1",
                     "bcast2",
                     "bcast3")
  val numNodes = actors.length

  val dpor = false

  // Mapping from { actor name => contents of delivered messages, in order }
  val state : Map[String, Queue[String]] = HashMap() ++
              actors.map((_, new Queue[String]()))

  def shutdownCallback() : Unit = {
    state.values.map(v => v.clear())
  }

  def checkpointApplicationState() : Any = {
    val checkpoint = new HashMap[String, Queue[String]] ++
                     actors.map((_, new Queue[String]()))
    state.foreach {
      case (key, value) => checkpoint(key) ++= value
    }
    return checkpoint
  }

  def restoreCheckpoint(checkpoint: Any) = {
    state.values.map(v => v.clear())
    checkpoint.asInstanceOf[Map[String, Queue[String]]].foreach {
      case (key, value) => state(key) ++= value
    }
  }

  Instrumenter().registerShutdownCallback(shutdownCallback)
  Instrumenter().registerCheckpointCallbacks(checkpointApplicationState, restoreCheckpoint)

  // Checks FIFO delivery. See 3.9.2 of "Reliable and Secure Distributed
  // Programming".
  def invariant(current_trace: Seq[ExternalEvent], state: Map[String, Queue[String]]) : Boolean = {
    // Correct sequence of deliveries: either 0, 1, or 2 messages in FIFO
    // order.

    // Assumes that only one node sent messages
    // TODO(cs): allow multiple nodes to send messages: maintain a list per
    // node, and check each list
    val sends : Seq[Send] = current_trace flatMap {
      case s: Send => Some(s)
      case _ => None
    }

    val correct = sends.map(e => e.message.asInstanceOf[RBBroadcast].msg.data)

    // Only non-crashed nodes need to deliver those messages.
    // TODO(cs): what is the correctness condition for crash-recovery FIFO
    // broadcast? We assume crash-stop here.
    val crashed = current_trace flatMap {
      case Kill(actor) => Some(actor)
      case _ => None
    }

    // Only check nodes that have been started.
    val started = current_trace flatMap {
      case Start(_,name) => Some(name)
      case _ => None
    }

    for (actor <- started.filterNot(a => crashed.contains(a))) {
      val delivery_order = state(actor)
      if (!delivery_order.equals(correct)) {
        println(actor + " produced incorrect order:")
        for (d <- delivery_order) {
          println(d)
        }
        println("-------------")
        return false
      }
    }
    return true
  }

  if (dpor) {
    val trace = Array[ExternalEvent]() ++
      // Start Actors.
      actors.map(actor_name =>
        Start(Props.create(classOf[BroadcastNode], state(actor_name)), actor_name)) ++
      // Execute the interesting events.
      Array[ExternalEvent](
      Send("bcast0", RBBroadcast(DataMessage("Message1"))),
      Send("bcast0", RBBroadcast(DataMessage("Message2")))
    )

    val sched = new DPOR
    Instrumenter().scheduler = sched
    sched.run(trace)
    println("Returned to main with events")
  } else {
    val trace = Array[ExternalEvent]() ++
      // Start Actors.
      actors.map(actor_name =>
        Start(Props.create(classOf[BroadcastNode], state(actor_name)), actor_name)) ++
      // Execute the interesting events.
      Array[ExternalEvent](
      WaitQuiescence,
      Send("bcast0", RBBroadcast(DataMessage("Message1"))),
      //Kill("bcast2"),
      Send("bcast0", RBBroadcast(DataMessage("Message2"))),
      WaitQuiescence
    )

    println("Minimizing:")
    for (event <- trace) {
      println(event.toString)
    }

    val sched = new RandomScheduler(3)
    Instrumenter().scheduler = sched
    sched.setInvariant((current_trace: Seq[ExternalEvent]) => invariant(current_trace, state))

    // val test_oracle = new RandomScheduler(3)
    // test_oracle.setInvariant((current_trace: Seq[ExternalEvent]) => invariant(current_trace, state))
    // // val minimizer : Minimizer = new DDMin(test_oracle)
    // val minimizer : Minimizer = new LeftToRightRemoval(test_oracle)
    // val events = minimizer.minimize(trace)

    // val events = sched.peek(trace)

    val events = sched.explore(trace)
    sched.shutdown
    println("Returned to main with events")

    /*
    events match {
      case None => None
      case Some(event_trace) => {
        println("events: ")
        for (event <- event_trace) {
          println(event.toString)
        }

        println("================")
        for (peek <- List(true)) { // List(true, false)
          // println("Trying STSScheduler")
          // val sts = new StatelessTestOracle(() => new STSSched(event_trace, peek))
          val greedy = new StatelessTestOracle(() => new GreedyED(event_trace, 2))
          // Instrumenter().scheduler = sts
          greedy.setInvariant((current_trace: Seq[ExternalEvent]) => invariant(current_trace, state))
          val minimizer : Minimizer = new LeftToRightRemoval(greedy)
          val minimized = minimizer.minimize(trace)
          println("Minimized externals:")
          for (external <- minimized) {
            println(external)
          }
          println("================")
        }
      }
    }
    */

    //println("Shutting down")
    //sched.shutdown
    //println("Shutdown successful")
  }

  // TODO(cs): either remove this code from the Broadcast nodes, or add it to the scheduler.
  // Wait for execution to terminate.
  // implicit val timeout = Timeout(2 seconds)
  // while (fd.liveNodes.map(
  //        n => Await.result(n.ask(StillActiveQuery), 500 milliseconds).
  //             asInstanceOf[Boolean]).reduceLeft(_ | _)) {
  //   Thread sleep 500
  // }
  // system.shutdown()
}
