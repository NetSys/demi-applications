package akka.dispatch.verification

import akka.actor.{ Actor, ActorRef, DeadLetter }
import akka.actor.{ActorSystem, ExtendedActorSystem}
import akka.actor.Props
import akka.actor.FSM
import akka.actor.FSM.Timer
import akka.dispatch.verification._
import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.util.Random
import java.nio._

import com.typesafe.config.ConfigFactory

import sample.distributeddata._

import scalax.collection.mutable.Graph,
       scalax.collection.GraphEdge.DiEdge,
       scalax.collection.edge.LDiEdge

class DDMessageFingerprinter extends MessageFingerprinter {
  override def fingerprint(msg: Any) : Option[MessageFingerprint] = {
    val alreadyFingerprint = super.fingerprint(msg)
    if (alreadyFingerprint != None) {
      return alreadyFingerprint
    }
    return None
  }
}

object Init {
  def externalMessageFilter(msg: Any) = {
    msg match {
      case _ => false
    }
  }
}

object Main extends App {
  //Instrumenter().setLogLevel("ERROR")

  EventTypes.setExternalMessageFilter(Init.externalMessageFilter)

  val config = ConfigFactory.parseString(
    s"""
    |akka.actor.guardian-supervisor-strategy = akka.actor.StoppingSupervisorStrategy
    |akka.loglevel = "DEBUG"
    |akka.stdout-loglevel = "DEBUG"
    |akka.actor.provider = akka.cluster.ClusterActorRefProvider
    |akka.cluster.seed-nodes = []
    """.stripMargin)

  Instrumenter().setPassthrough
  Instrumenter().actorSystem(Some(config))
  Instrumenter().unsetPassthrough
  println("Done initializing ActorSystem")

  val ddChecks = new DDChecks

  val fingerprintFactory = new FingerprintFactory
  fingerprintFactory.registerFingerprinter(new DDMessageFingerprinter)

  def shutdownCallback() = {
    ddChecks.clear
  }

  Instrumenter().registerShutdownCallback(shutdownCallback)

  val schedulerConfig = SchedulerConfig(
    messageFingerprinter=fingerprintFactory,
    enableFailureDetector=false,
    enableCheckpointing=true,
    shouldShutdownActorSystem=true,
    filterKnownAbsents=false,
    invariant_check=Some(ddChecks.invariant),
    ignoreTimers=false,
    storeEventTraces=true
  )

  // XXX

  val prefix = Seq(
    Start(() => Props.create(classOf[ShoppingCart], "user1"), "s1"),
    Start(() => Props.create(classOf[ShoppingCart], "user2"), "s2")
  )
  val weights = new FuzzerWeights(kill=0.00, send=0.3, wait_quiescence=0.0,
                                  partition=0.0, unpartition=0)
  // XXX
  //val messageGen = new ClientMessageGenerator(members)
  val messageGen = null
  val fuzzer = new Fuzzer(0, weights, messageGen, prefix)

  val fuzz = true

  var traceFound: EventTrace = null
  var violationFound: ViolationFingerprint = null
  var depGraph : Graph[Unique, DiEdge] = null
  var initialTrace : Queue[Unique] = null
  var filteredTrace : Queue[Unique] = null

  if (fuzz) {
    def replayerCtor() : ReplayScheduler = {
      val replayer = new ReplayScheduler(schedulerConfig)
      return replayer
    }
    def randomiziationCtor() : RandomizationStrategy = {
      //return new FullyRandom(userDefinedFilter)
      return new SrcDstFIFO()
    }
    val tuple = RunnerUtils.fuzz(fuzzer, ddChecks.invariant,
                                 schedulerConfig,
                                 validate_replay=Some(replayerCtor),
                                 maxMessages=Some(3000),  // XXX
                                 invariant_check_interval=30,
                                 randomizationStrategyCtor=randomiziationCtor)
    traceFound = tuple._1
    violationFound = tuple._2
    depGraph = tuple._3
    initialTrace = tuple._4
    filteredTrace = tuple._5
  }

  if (fuzz) {
    // XXX var provenanceTrace = traceFound.intersection(filteredTrace, fingerprintFactory)

    val serializer = new ExperimentSerializer(
      fingerprintFactory,
      new BasicMessageSerializer)

    val dir = serializer.record_experiment("dd-fuzz",
      traceFound.filterCheckpointMessages(), violationFound,
      depGraph=Some(depGraph), initialTrace=Some(initialTrace),
      filteredTrace=Some(filteredTrace))

    val (mcs, stats1, verified_mcs, violation) =
    RunnerUtils.stsSchedDDMin(false,
      schedulerConfig,
      traceFound,
      violationFound,
      actorNameProps=Some(ExperimentSerializer.getActorNameProps(traceFound)))

    if (verified_mcs.isEmpty) {
      throw new RuntimeException("MCS wasn't validated")
    }
    val mcs_dir = serializer.serializeMCS(dir, mcs, stats1, verified_mcs, violation, false)
    println("MCS DIR: " + mcs_dir)
  } else { // !fuzz
    val dir =
    "experiments/akka-raft-fuzz-long_2015_08_30_20_59_21"
    val mcs_dir =
    "experiments/akka-raft-fuzz-long_2015_08_30_20_59_21_DDMin_STSSchedNoPeek"

    val msgSerializer = new BasicMessageSerializer
    val msgDeserializer = new BasicMessageDeserializer

    def shouldRerunDDMin(externals: Seq[ExternalEvent]) =
      externals.exists({
        case _ => false
      })

    RunnerUtils.runTheGamut(dir, mcs_dir, schedulerConfig, msgSerializer,
      msgDeserializer, shouldRerunDDMin=shouldRerunDDMin)
  }
}
