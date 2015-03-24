import broadcast._
import akka.actor.{ Actor, ActorRef, DeadLetter }
import akka.actor.ActorSystem
import akka.actor.Props
import scala.util.Random
import akka.dispatch.verification._
import scala.collection.mutable.HashSet
import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map

import scalax.collection.mutable.Graph,
       scalax.collection.GraphEdge.DiEdge,
       scalax.collection.edge.LDiEdge

case class BroadcastViolation(fingerprint: String, nodes: Seq[String]) extends ViolationFingerprint {
  def matches(other: ViolationFingerprint) : Boolean = {
    other match {
      case BroadcastViolation(f, n) => return f == fingerprint && nodes == n
      case _ => return false
    }
  }
  def affectedNodes = nodes
}

class BroadcastMessageFingerprinter extends MessageFingerprinter {
  override def fingerprint(msg: Any) : Option[MessageFingerprint] = {
    val alreadyFingerprint = super.fingerprint(msg)
    if (alreadyFingerprint != None) {
      return alreadyFingerprint
    }

    val str = msg match {
      // Ignore ids! Not sure this is exactly the right thing to do, but...
      case DataMessage(data) =>
        ("DataMessage", data).toString
      case RBBroadcast(DataMessage(data)) =>
        ("RBBroadcast", data).toString
      case SLDeliver(snd, DataMessage(data)) =>
        ("SLDeliver", snd, data).toString
      case ACK(snd, _) =>
        ("ACK", snd).toString
      case m =>
        ""
    }

    if (str != "") {
      return Some(BasicFingerprint(str))
    }
    return None
  }
}

class ClientMessageGenerator(actors: Seq[String]) extends MessageGenerator {
  val wordsUsedSoFar = new HashSet[String]
  val rand = new Random
  val destinations = new RandomizedHashSet[String]
  for (dst <- actors) {
    destinations.insert(dst)
  }

  def generateMessage(alive: RandomizedHashSet[String]) : Send = {
    val dst = destinations.getRandomElement()
    // TODO(cs): 10000 is a bit arbitrary, and this algorithm fails
    // disastrously as we start to approach 10000 Send events.
    var word = rand.nextInt(10000).toString
    while (wordsUsedSoFar contains word) {
      word = rand.nextInt(10000).toString
    }
    wordsUsedSoFar += word
    return Send(dst, () =>  RBBroadcast(DataMessage(word)))
  }
}

object Main extends App {
  val actors = Seq("bcast0",
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
  def invariant(current_trace: Seq[ExternalEvent], state: Map[String, Queue[String]]) : Option[akka.dispatch.verification.ViolationFingerprint]= {
    // Correct sequence of deliveries: either 0, 1, or 2 messages in FIFO
    // order.

    // Assumes that only one node sent messages
    // TODO(cs): allow multiple nodes to send messages: maintain a list per
    // node, and check each list
    val sends : Seq[Send] = current_trace flatMap {
      case s: Send => Some(s)
      case _ => None
    }

    val correct = sends.map(e => e.messageCtor().asInstanceOf[RBBroadcast].msg.data)

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
        return Some(BroadcastViolation("" + delivery_order, Seq(actor)))
      }
    }
    return None
  }

  val prefix = Array[ExternalEvent]() ++
    // Start Actors.
    actors.map(actor_name =>
      Start(() => Props.create(classOf[BroadcastNode], state(actor_name), Some(actors)), actor_name)) ++
    // Execute the interesting events.
    Array[ExternalEvent](
    WaitQuiescence(),
    Send("bcast0", () => RBBroadcast(DataMessage("Message1"))),
    //Kill("bcast2"),
    Send("bcast0", () => RBBroadcast(DataMessage("Message2"))),
    WaitQuiescence()
  )

  val wrappedInvariant = {
    (current_trace: Seq[ExternalEvent], notUsed: HashMap[String,Option[CheckpointReply]]) =>
     invariant(current_trace, state)
  }

  val fingerprintFactory = new FingerprintFactory
  fingerprintFactory.registerFingerprinter(new BroadcastMessageFingerprinter)

  val fuzz = false

  var traceFound: EventTrace = null
  var violationFound: ViolationFingerprint = null
  var depGraph : Graph[Unique, DiEdge] = null
  var initialTrace : Queue[Unique] = null
  var filteredTrace : Queue[Unique] = null
  if (fuzz) {
    val weights = new FuzzerWeights(kill=0.0, send=0.3, wait_quiescence=0.1,
                                    partition=0.0, unpartition=0)
    val messageGen = new ClientMessageGenerator(actors)
    val fuzzer = new Fuzzer(500, weights, messageGen, prefix)

    //def replayerCtor() : ReplayScheduler = {
    //  val replayer = new ReplayScheduler(fingerprintFactory, false, false)
    //  replayer.setEventMapper(Init.eventMapper)
    //  return replayer
    //}
    val tuple = RunnerUtils.fuzz(fuzzer, wrappedInvariant,
                                 fingerprintFactory,
                                 invariant_check_interval=200)
                                 //validate_replay=Some(replayerCtor))
    traceFound = tuple._1
    violationFound = tuple._2
    depGraph = tuple._3
    initialTrace = tuple._4
    filteredTrace = tuple._5
  }

  val serializer = new ExperimentSerializer(
    fingerprintFactory,
    new BasicMessageSerializer)

  val dir = if (fuzz) serializer.record_experiment("rbcast-fuzz",
    traceFound.filterCheckpointMessages(), violationFound,
    depGraph=Some(depGraph), initialTrace=Some(initialTrace),
    filteredTrace=Some(filteredTrace)) else
    "/Users/cs/Research/UCB/code/sts2-applications/experiments/akka-raft-fuzz_2015_03_14_01_08_35"


  println("Trying STSSchedDDMin")
  // Allow peek:
  var (mcs3, stats3, mcs_execution3, violation3) =
    RunnerUtils.stsSchedDDMin(dir,
      fingerprintFactory,
      new BasicMessageDeserializer,
      true,
      wrappedInvariant)

  serializer.serializeMCS(dir, mcs3, stats3, mcs_execution3, violation3)

  var (mcs5, stats5, mcs_execution5, violation5) =
    RunnerUtils.editDistanceDporDDMin(dir,
      fingerprintFactory,
      new BasicMessageDeserializer,
      wrappedInvariant)

  serializer.serializeMCS(dir, mcs5, stats5, mcs_execution5, violation5)

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
