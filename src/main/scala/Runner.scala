import broadcast.{ BroadcastNode, RBBroadcast, DataMessage }
import akka.actor.{ Actor, ActorRef, DeadLetter }
import akka.actor.ActorSystem
import akka.actor.Props
import akka.dispatch.verification._

object Main extends App {
  val actors = Array("bcast0",
                     "bcast1",
                     "bcast2",
                     "bcast3")
  val numNodes = actors.length

  val trace = Array[ExternalEvent]() ++
    // Start Actors.
    actors.map(actor_name =>
      Start(Props.create(classOf[BroadcastNode]), actor_name)) ++
    // Execute the interesting events.
    Array[ExternalEvent](
    //WaitQuiescence,
    Send("bcast0", RBBroadcast(DataMessage("Message1"))) //,
    //Kill("bcast2"),
    //WaitQuiescence
  )

  // val sched = new PeekScheduler
  val sched = new DPOR
  Instrumenter().scheduler = sched
  // val events = sched.peek(trace)
  sched.run(trace)
  println("Returned to main with events")
  println("Shutting down")
  //sched.shutdown
  println("Shutdown successful")

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
