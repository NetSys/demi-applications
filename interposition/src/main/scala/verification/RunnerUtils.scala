package akka.dispatch.verification


// Utilities for writing Runner.scala files.
object RunnerUtils {

  def fuzz(fuzzer: Fuzzer, invariant: TestOracle.Invariant, shutdownCallback: () => Any) :
        Tuple2[EventTrace, ViolationFingerprint] = {
    var violationFound : ViolationFingerprint = null
    var traceFound : EventTrace = null
    while (violationFound == null) {
      val fuzzTest = fuzzer.generateFuzzTest()
      println("Trying: " + fuzzTest)

      val sched = new RandomScheduler(1, false, 30, false)
      sched.setInvariant(invariant)
      Instrumenter().scheduler = sched
      sched.explore(fuzzTest) match {
        case None =>
          println("Returned to main with events")
          sched.shutdown()
          println("shutdown successfully")
          shutdownCallback()
        case Some((trace, violation)) => {
          println("Found a safety violation!")
          violationFound = violation
          traceFound = trace
          sched.shutdown()
        }
      }
    }

    return (traceFound, violationFound)
  }

  def deserializeExperiment(experiment_dir: String,
      messageDeserializer: MessageDeserializer,
      scheduler: ExternalEventInjector[_] with Scheduler):
                  Tuple2[EventTrace, ViolationFingerprint] = {
    val deserializer = new ExperimentDeserializer(experiment_dir)
    Instrumenter().scheduler = scheduler
    scheduler.populateActorSystem(deserializer.get_actors)
    val violation = deserializer.get_violation(messageDeserializer)
    val trace = deserializer.get_events(messageDeserializer, Instrumenter().actorSystem)
    return (trace, violation)
  }

  def replayExperiment(experiment_dir: String,
                       messageFingerprinter: MessageFingerprinter,
                       messageDeserializer: MessageDeserializer) : EventTrace = {
    val replayer = new ReplayScheduler(messageFingerprinter, false, false)
    val (trace, _) = RunnerUtils.deserializeExperiment(experiment_dir, messageDeserializer, replayer)

    println("Trying replay:")
    val events = replayer.replay(trace)
    println("Done with replay")
    replayer.shutdown
    return events
  }

  def randomDDMin(experiment_dir: String,
                  messageFingerprinter: MessageFingerprinter,
                  messageDeserializer: MessageDeserializer,
                  invariant: TestOracle.Invariant) :
        Tuple4[Seq[ExternalEvent], MinimizationStats, Option[EventTrace], ViolationFingerprint] = {
    val sched = new RandomScheduler(1, false, 0, false)
    sched.setInvariant(invariant)
    val (trace, violation) = RunnerUtils.deserializeExperiment(experiment_dir, messageDeserializer, sched)
    sched.setMaxMessages(trace.size)

    val ddmin = new DDMin(sched, false)
    val mcs = ddmin.minimize(trace.original_externals, violation)
    println("Validing MCS...")
    val validated_mcs = ddmin.verify_mcs(mcs, violation)
    return (mcs, ddmin.stats, validated_mcs, violation)
  }

  def stsSchedDDMin(experiment_dir: String,
                    messageFingerprinter: MessageFingerprinter,
                    messageDeserializer: MessageDeserializer,
                    allowPeek: Boolean,
                    invariant: TestOracle.Invariant,
                    event_mapper: Option[HistoricalScheduler.EventMapper]) :
        Tuple4[Seq[ExternalEvent], MinimizationStats, Option[EventTrace], ViolationFingerprint] = {
    val sched = new STSScheduler(new EventTrace, allowPeek,
        messageFingerprinter, false)
    sched.setInvariant(invariant)
    event_mapper match {
      case Some(f) => sched.setEventMapper(f)
      case None => None
    }
    val (trace, violation) = RunnerUtils.deserializeExperiment(experiment_dir, messageDeserializer, sched)
    sched.original_trace = trace

    val ddmin = new DDMin(sched)
    val mcs = ddmin.minimize(trace.original_externals, violation)
    println("Validing MCS...")
    val validated_mcs = ddmin.verify_mcs(mcs, violation)
    return (mcs, ddmin.stats, validated_mcs, violation)
  }
}