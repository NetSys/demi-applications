import akka.actor.{ Actor, ActorRef, DeadLetter }
import akka.actor.ActorSystem
import akka.actor.Props
import akka.dispatch.verification._
import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.Map
import scala.util.Random
import pl.project13.scala.akka.raft.example._
import pl.project13.scala.akka.raft.protocol._
import pl.project13.scala.akka.raft.example.protocol._
import pl.project13.scala.akka.raft._

class ClientMessageGenerator(raft_members: Seq[String]) extends MessageGenerator {
  val wordsUsedSoFar = new HashSet[String]
  val rand = new Random
  val destinations = new RandomizedHashSet[String]
  for (dst <- raft_members) {
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
    return Send(dst, () =>
      ClientMessage[AppendWord](Instrumenter().actorSystem.deadLetters, AppendWord(word)))
  }
}

object Main extends App {
  // Correctness properties of Uniform Consensus:
  // ------------
  // Termination: Every correct process eventually decides some value.
  // Validity: If a process decides v, then v was proposed by some process.
  // Integrity: No process decides twice.
  // Uniform agreement: No two processes decide differently.
  // ------------
  // Correctness properties of Epoch Change:
  // ------------
  // Monotonicity: If a correct process starts an epoch (ts, l) and later
  //               starts an epoch (ts′, l′), then ts′ > ts.
  // Consistency: If a correct process starts an epoch (ts, l) and another
  //              correct process starts an epoch (ts′, l′) with ts = ts′, then l = l′.
  // Eventual leadership: There is a time after which every correct process
  //             has started some epoch and starts no further epoch, such that the last epoch
  //             started at every correct process is epoch (ts, l) and process l is correct.
  // ------------
  // Correctness properties of Epoch Consensus:
  // ------------
  // Validity: If a correct process ep-decides v, then v was ep-proposed
  //    by the leader l′ of some epoch consensus with timestamp ts′ ≤ ts and leader l′.
  // Uniform agreement: No two processes ep-decide differently.
  // Integrity: Every correct process ep-decides at most once.
  // Lock-in: If a correct process has ep-decided v in an epoch consensus
  //    with timestamp ts′ < ts, then no correct process ep-decides a value
  //    different from v.
  // Termination: If the leader l is correct, has ep-proposed a value, and
  //   no correct process aborts this epoch consensus, then every correct process
  //   eventually ep-decides some value.
  // Abort behavior: When a correct process aborts an epoch consensus, it
  //   eventu- ally will have completed the abort; moreover, a correct process
  //   completes an abort only if the epoch consensus has been aborted by some
  //   correct process.
  // -----------
  // The safety conditions according to the Raft paper are:
  // -----------
  // Election Safety: at most one leader can be elected in a given term. §5.2
  // Leader Append-Only: a leader never overwrites or deletes entries in its log;
  //     it only appends new entries. §5.3
  // LogMatching: if two logs contain an entry with the same index and term, then
  //     the logs are identical in all entries up through the given index. §5.3
  // Leader Completeness: if a log entry is committed in a given term, then that
  //     entry will be present in the logs of the leaders for all higher-numbered
  //     terms. §5.4
  // StateMachine Safety: if a server has applied a log entry at a given index to
  //     its state machine, no other server will ever apply a different log entry for
  //     the same index. §5.4.3
  // -------------
  // + A simple one: no node should crash.

  def invariant(seq: Seq[ExternalEvent], checkpoint: HashMap[String,CheckpointReply]) : Option[ViolationFingerprint] = {
    println(checkpoint)
    return None
  }

  val members = (1 to 3) map { i => s"raft-member-$i" }

  val prefix = Array[ExternalEvent]() ++
    //Array[ExternalEvent](Start(() =>
    //  RaftClientActor.props(Instrumenter().actorSystem() / "raft-member-*"), "client")) ++
    members.map(member =>
      Start(() => Props.create(classOf[WordConcatRaftActor]), member)) ++
    members.map(member =>
      Send(member, () => {
        val clusterRefs = Instrumenter().actorMappings.filter({
            case (k,v) => k != "client" && !ActorTypes.systemActor(k)
        }).values
        ChangeConfiguration(ClusterConfiguration(clusterRefs))
      })) ++
    Array[ExternalEvent](
    WaitQuiescence,
    WaitTimers(1)
    //WaitQuiescence
    // Continue(500)
  )

  val weights = new FuzzerWeights(0.01, 0.3, 0.3, 0.0, 0.05, 0.05, 0.2)
  val messageGen = new ClientMessageGenerator(members)
  val fuzzer = new Fuzzer(30, weights, messageGen, prefix)
  val fuzzTest = fuzzer.generateFuzzTest()
  println(fuzzTest)

  val sched = new RandomScheduler(1, false)
  sched.setInvariant(invariant)
  Instrumenter().scheduler = sched
  sched.explore(fuzzTest)
  println("Returned to main with events")
  sched.shutdown()
  println("shutdown successfully")
}
