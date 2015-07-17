package akka.dispatch.verification

import com.typesafe.config.ConfigFactory
import akka.actor.{Actor, Cell, ActorRef, ActorSystem, Props}

import akka.dispatch.Envelope

import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.SynchronizedQueue
import scala.collection.mutable.Set
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashSet
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.collection.generic.Growable

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean
import java.util.Random

import org.slf4j.LoggerFactory,
       ch.qos.logback.classic.Level,
       ch.qos.logback.classic.Logger

/**
 * Takes a list of ExternalEvents as input, and explores random interleavings
 * of internal messages until either a maximum number of interleavings is
 * reached, or a given invariant is violated.
 *
 * If invariant_check_interval is <=0, only checks the invariant at the end of
 * the execution. Otherwise, checks the invariant every
 * invariant_check_interval message deliveries.
 *
 * max_executions determines how many executions we will try before giving
 * up.
 *
 * Additionally records internal and external events that occur during
 * executions that trigger violations.
 */
class RandomScheduler(val schedulerConfig: SchedulerConfig,
                      max_executions:Int=1,
                      invariant_check_interval:Int=0,
                      randomizationStrategy:RandomizationStrategy=new FullyRandom)
    extends AbstractScheduler with ExternalEventInjector[ExternalEvent] with TestOracle {
  def getName: String = "RandomScheduler"

  val messageFingerprinter = schedulerConfig.messageFingerprinter

  val logger = LoggerFactory.getLogger("RandomScheduler")

  // Allow the user to place a bound on how many messages are delivered.
  // Useful for dealing with non-terminating systems.
  var maxMessages = Int.MaxValue
  def setMaxMessages(_maxMessages: Int) = {
    maxMessages = _maxMessages
  }

  var test_invariant : Invariant = schedulerConfig.invariant_check match {
    case Some(i) => i
    case None => null
  }

  // Current set of enabled events.
  // Our use of Uniq and Unique is somewhat confusing. Uniq is used to
  // associate MsgSends with their subsequent MsgEvents.
  // Unique is used by DepTracker.
  val pendingEvents : RandomizationStrategy = randomizationStrategy

  // Current set of failure detector or CheckpointRequest messages destined for
  // actors, to be delivered in the order they arrive.
  // Always prioritized over internal messages.
  var pendingSystemMessages = new Queue[Uniq[(Cell, Envelope)]]

  // The violation we're looking for, if not None.
  var lookingFor : Option[ViolationFingerprint] = None

  // If we're looking for a specific violation, this is just used a boolean
  // flag: if not None, then we've found what we're looking for.
  // Otherwise, it will contain the first safety violation we found.
  var violationFound : Option[ViolationFingerprint] = None

  // The trace we're exploring
  var trace : Seq[ExternalEvent] = null

  // how many non-checkpoint messages we've scheduled so far.
  var messagesScheduledSoFar = 0

  // what was the last value of messagesScheduledSoFar we took a checkpoint at.
  var lastCheckpoint = 0

  // How many times we've replayed
  var stats: MinimizationStats = null

  // For every message we deliver, track which messages become enabled as a
  // result of our delivering that message. This can be used later to recreate
  // the DepGraph (used by DPOR).
  var depTracker = new DepTracker(messageFingerprinter)

  // Avoid infinite loops with timers: if we just scheduled one,
  // don't schedule the exact same one immediately again until we've scheduled
  // some other message first.
  //
  // Tuple is : (receiver, timer object)
  //
  // Invariant: if a pending timer is contained in this HashSet, it should
  // never also be contained in pendingEvents. i.e., placing a timer
  // in justScheduledTimers should effectively `decommision` it.
  val justScheduledTimers = new HashSet[(String, Any)]

  // if a timer was retriggered while it was also contained in
  // justScheduledTimers, put it here rather than pendingEvents.
  val timersToResend = new SynchronizedQueue[(String, Any)]

  // Tell ExternalEventInjector to notify us whenever a WaitQuiescence has just
  // caused us to arrive at Quiescence.
  setQuiescenceCallback(() => {
    if (event_orchestrator.previous_event.getClass == classOf[WaitQuiescence]) {
      depTracker.reportQuiescence(event_orchestrator.previous_event.asInstanceOf[WaitQuiescence])
    }
  })
  // Tell EventOrchestrator to tell us about Kills, Parititions, UnPartitions
  event_orchestrator.setKillCallback(depTracker.reportKill)
  event_orchestrator.setPartitionCallback(depTracker.reportPartition)
  event_orchestrator.setUnPartitionCallback(depTracker.reportUnPartition)

  /**
   * If we're looking for a specific violation, return None if the given
   * violation doesn't match, or Some(violation) if it does.
   *
   * If we're not looking for a specific violation, return the given
   * violation.
   */
  private[this] def violationMatches(violation: Option[ViolationFingerprint]) : Option[ViolationFingerprint] = {
    lookingFor match {
      case None =>
        return violation
      case Some(original_fingerprint) =>
        violation match {
          case None =>
            return None
          case Some(fingerprint) =>
            if (original_fingerprint.matches(fingerprint)) {
              return lookingFor
            } else {
              return None
            }
        }
    }
  }

  private[this] def checkIfBugFound(event_trace: EventTrace): Option[(EventTrace, ViolationFingerprint)] = {
    violationFound match {
      // If the violation has already been found, return.
      case Some(fingerprint) =>
        // Prune off any external events that we didn't end up using.
        event_trace.original_externals =
          event_trace.original_externals.slice(0, event_orchestrator.traceIdx)
        return Some((event_trace, fingerprint))
      // Else, check the invariant condition one last time.
      case None =>
        var checkpoint = new HashMap[String, Option[CheckpointReply]]
        if (schedulerConfig.enableCheckpointing) {
          checkpoint = takeCheckpoint()
        }
        val violation = test_invariant(trace, checkpoint)
        violationFound = violationMatches(violation)
        violationFound match {
          case Some(fingerprint) =>
            return Some((event_trace, fingerprint))
          case None => None
        }
    }
    return None
  }

  // Explore exactly one execution, invoke terminationCallback when the
  // execution has finished.
  def nonBlockingExplore(_trace: Seq[ExternalEvent],
                         terminationCallback: (Option[(EventTrace,ViolationFingerprint)]) => Any) {
    nonBlockingExplore(_trace, None, terminationCallback)
  }

  def nonBlockingExplore(_trace: Seq[ExternalEvent],
                         _lookingFor: Option[ViolationFingerprint],
                         terminationCallback: (Option[(EventTrace,ViolationFingerprint)]) => Any) {
    if (!(Instrumenter().scheduler eq this)) {
      throw new IllegalStateException("Instrumenter().scheduler not set!")
    }
    trace = _trace
    lookingFor = _lookingFor

    if (test_invariant == null) {
      throw new IllegalArgumentException("Must invoke setInvariant before test()")
    }

    event_orchestrator.events.setOriginalExternalEvents(_trace)
    if (stats != null) {
      stats.increment_replays()
    }

    execute_trace(_trace, Some((event_trace: EventTrace) => {
      val ret = checkIfBugFound(event_trace)
      terminationCallback(ret)
    }))
  }

  /**
   * Given an external event trace, randomly explore executions involving those
   * external events.
   *
   * Returns a trace of the internal and external events observed if a failing
   * execution was found, along with a `fingerprint` of the safety violation.
   * otherwise returns None if no failure was triggered within max_executions.
   *
   * Callers should call shutdown() sometime after this method returns if they
   * want to invoke any other methods.
   *
   * Precondition: setInvariant has been invoked.
   */
  def explore (_trace: Seq[ExternalEvent]) : Option[(EventTrace, ViolationFingerprint)] = {
    return explore(_trace, None, None)
  }

  /**
   * if looking_for is not None, only look for an invariant violation that
   * matches looking_for
   */
  def explore (_trace: Seq[ExternalEvent],
               _lookingFor: Option[ViolationFingerprint],
               terminationCallback: Option[(Option[(EventTrace,ViolationFingerprint)])=>Any]=None) :
       Option[(EventTrace, ViolationFingerprint)] = {
    if (!(Instrumenter().scheduler eq this)) {
      throw new IllegalStateException("Instrumenter().scheduler not set!")
    }
    trace = _trace
    lookingFor = _lookingFor

    if (test_invariant == null) {
      throw new IllegalArgumentException("Must invoke setInvariant before test()")
    }

    for (i <- 1 to max_executions) {
      println("Trying random interleaving " + i)
      event_orchestrator.events.setOriginalExternalEvents(_trace)
      if (stats != null) {
        stats.increment_replays()
      }

      val event_trace = execute_trace(_trace)
      checkIfBugFound(event_trace) match {
        case Some((event_trace, fingerprint)) =>
          return Some((event_trace, fingerprint))
        case None =>
      }

      if (i != max_executions) {
        // 'Tis a lesson you should heed: Try, try, try again.
        // If at first you don't succeed: Try, try, try again
        reset_all_state
      }
    }
    // No bug found...
    return None
  }

  override def event_produced(cell: Cell, envelope: Envelope) = {
    var snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msg = envelope.message
    assert(started.get(), "!started.get(): " + snd + " -> " + rcv + " " + msg)
    if (logger.isTraceEnabled()) {
      logger.trace("event_produced: " + snd + " -> " + rcv + " " + msg)
    }

    val uniq = Uniq[(Cell, Envelope)]((cell, envelope))
    var isTimer = false

    handle_event_produced(snd, rcv, envelope) match {
      case InternalMessage => {
        if (snd == "deadLetters") {
          isTimer = true
        }
        val unique = depTracker.reportNewlyEnabled(snd, rcv, msg)
        if (!crosses_partition(snd, rcv)) {
          pendingEvents += ((uniq, unique))
        }
      }
      case ExternalMessage => {
        if (MessageTypes.fromFailureDetector(msg) ||
            MessageTypes.fromCheckpointCollector(msg)) {
          pendingSystemMessages += uniq
        } else {
          val unique = depTracker.reportNewlyEnabledExternal(snd, rcv, msg)
          pendingEvents += ((uniq, unique))
        }
      }
      case FailureDetectorQuery => None
      case CheckpointReplyMessage =>
        if (checkpointer.done && !blockedOnCheckpoint.get) {
          val violation = test_invariant(trace, checkpointer.checkpoints)
          require(violationFound == None)
          violationFound = violationMatches(violation)
        }
    }

    // Record this MsgSend as a special if it was sent from a timer.
    snd = if (isTimer) "Timer" else snd
    event_orchestrator.events.appendMsgSend(snd, rcv, envelope.message, uniq.id)
  }

  // Record a mapping from actor names to actor refs
  override def event_produced(event: Event) = {
    super.event_produced(event)
    handle_spawn_produced(event)
  }

  // Record that an event was consumed
  override def event_consumed(event: Event) = {
    handle_spawn_consumed(event)
  }

  // Record a message send event
  override def event_consumed(cell: Cell, envelope: Envelope) = {
    handle_event_consumed(cell, envelope)
  }

  // Return true if the current event is a wait condition and the condition
  // yields true.
  def checkWaitCondition(): Boolean = {
    if (event_orchestrator.trace_finished) {
      return false
    }
    event_orchestrator.current_event match {
      case WaitCondition(cond) =>
        return cond()
      case _ => return false
    }
  }

  def schedule_new_message(blockedActors: scala.collection.immutable.Set[String]) : Option[(Cell, Envelope)] = {
    // First, check if we've found the violation. If so, stop.
    violationFound match {
      case Some(fingerprint) =>
        return None
      case None =>
        None
    }

    // Also check if the WaitCondition is true. If so, quiesce.
    if (checkWaitCondition) {
      return None
    }

    // Also check if we've exceeded our message limit
    if (messagesScheduledSoFar > maxMessages) {
      println("Exceeded maxMessages")
      event_orchestrator.finish_early
      return None
    }

    // Otherwise, see if it's time to check the invariant violation.
    if (invariant_check_interval > 0 &&
        (messagesScheduledSoFar % invariant_check_interval) == 0 &&
        !blockedOnCheckpoint.get() &&
        lastCheckpoint != messagesScheduledSoFar) {
      println("Checking invariant")

      if (!schedulerConfig.enableCheckpointing) {
        // If no checkpointing, go ahead and check the invariant now
        var checkpoint = new HashMap[String, Option[CheckpointReply]]
        val violation = test_invariant(trace, checkpoint)
        violationFound = violationMatches(violation)
        // Return early if we found one.
        violationFound match {
          case Some(fingerprint) =>
            return None
          case None =>
            None
        }
      } else {
        // Otherwise we check the invariant once we have received all
        // CheckpointReplies.
        lastCheckpoint = messagesScheduledSoFar
        prepareCheckpoint()
      }
    }

    // Invoked when we're about to schedule a new message (either a repeating
    // timer or a normal message)
    def updateRepeatingTimer(aboutToDeliver: (Cell, Envelope)) {
      if (Instrumenter().isTimer(
            aboutToDeliver._1.self.path.name, aboutToDeliver._2.message)) {
        justScheduledTimers += ((aboutToDeliver._1.self.path.name,
                                 aboutToDeliver._2.message))
      } else {
        // We're about to deliver a non-timer! Resend all of
        // the repeating timers.
        // TODO(cs): I don't know if this screws up depGraph...
        timersToResend foreach { case (rcv, timer) => handle_timer(rcv, timer) }
        timersToResend.clear
        justScheduledTimers.clear
      }
    }

    // Proceed normally.
    send_external_messages()
    // Always prioritize system messages.
    if (!pendingSystemMessages.isEmpty) {
      // Find a non-blocked destination
      Util.find_non_blocked_message[Uniq[(Cell, Envelope)]](
        blockedActors,
        pendingSystemMessages,
        () => pendingSystemMessages.dequeue(),
        (e: Uniq[(Cell, Envelope)]) => e.element._1.self.path.name) match {
        case Some(uniq) =>
          event_orchestrator.events.appendMsgEvent(uniq.element, uniq.id)
          updateRepeatingTimer(uniq.element)
          return Some(uniq.element)
        case None =>
      }
    }

    // Find a non-blocked destination
    Util.find_non_blocked_message[Tuple2[Uniq[(Cell,Envelope)],Unique]](
      blockedActors,
      pendingEvents,
      () => pendingEvents.removeRandomElement,
      (e: Tuple2[Uniq[(Cell,Envelope)],Unique]) => e._1.element._1.self.path.name) match {
      case Some((uniq,  unique)) =>
        messagesScheduledSoFar += 1
        if (messagesScheduledSoFar == Int.MaxValue) {
          messagesScheduledSoFar = 1
        }

        event_orchestrator.events.appendMsgEvent(uniq.element, uniq.id)
        depTracker.reportNewlyDelivered(unique)

        if (logger.isTraceEnabled()) {
          val cell = uniq.element._1
          val envelope = uniq.element._2
          val snd = envelope.sender.path.name
          val rcv = cell.self.path.name
          val msg = envelope.message
          logger.trace("schedule_new_message("+unique.id+"): " + snd + " -> " + rcv + " " + msg)
        }

        updateRepeatingTimer(uniq.element)
        return Some(uniq.element)
      case None =>
        return None
    }
  }

  override def notify_quiescence () {
    violationFound match {
      case None => handle_quiescence
      case Some(fingerprint) =>
        // Wake up the main thread early; no need to continue with the rest of
        // the trace.
        println("Violation found early. Halting")
        started.set(false)
        terminationCallback match {
          case None => traceSem.release
          case Some(f) => f(event_orchestrator.events)
        }
    }
  }

  // Shutdown the scheduler, this ensures that the instrumenter is returned to its
  // original pristine form, so one can change schedulers
  override def shutdown () = {
    handle_shutdown
  }

  // Notification that the system has been reset
  override def start_trace() : Unit = {
    handle_start_trace
  }

  override def before_receive(cell: Cell) : Unit = {
    handle_before_receive(cell)
  }

  override def after_receive(cell: Cell) : Unit = {
    handle_after_receive(cell)
  }

  def setInvariant(invariant: Invariant) {
    test_invariant = invariant
  }

  def notify_timer_cancel(rcv: ActorRef, msg: Any): Unit = {
    if (handle_timer_cancel(rcv, msg)) {
      return
    }
    // Awkward, we need to walk through the entire hashset to find what we're
    // looking for.
    pendingEvents.remove("deadLetters",rcv.path.name, msg)
  }

  override def actorTerminated(name: String): Seq[(String, Any)] = {
    // TODO(cs): also deal with pendingSystemMessages
    return randomizationStrategy.removeAll(name).map {
      case (uniq, unique) =>
        val envelope = uniq.element._2
        val snd = envelope.sender.path.name
        val msg = envelope.message
        (snd, msg)
    }
  }

  override def enqueue_timer(receiver: String, msg: Any) {
    if (justScheduledTimers contains ((receiver, msg))) {
      // We just scheduled this timer, don't yet put it in
      // pendingEvents! This is to avoid an infinite loop of scheduling
      // timers.
      timersToResend += ((receiver, msg))
      return
    }

    handle_timer(receiver, msg)
  }

  override def handleMailboxIdle() {
    advanceTrace
  }

  override def reset_all_state () {
    // TODO(cs): also reset Instrumenter()'s state?
    reset_state(true)
    // N.B. important to clear our state after we invoke reset_state, since
    // it's possible that enqueue_message may be called during shutdown.
    super.reset_all_state
    justScheduledTimers.clear()
    timersToResend.clear()
    pendingEvents.clear()
    pendingSystemMessages = new Queue[Uniq[(Cell, Envelope)]]
    lookingFor = None
    violationFound = None
    trace = null
    messagesScheduledSoFar = 0
    lastCheckpoint = 0
    depTracker = new DepTracker(messageFingerprinter)
    event_orchestrator.setKillCallback(depTracker.reportKill)
    event_orchestrator.setPartitionCallback(depTracker.reportPartition)
    event_orchestrator.setUnPartitionCallback(depTracker.reportUnPartition)
  }

  def test(events: Seq[ExternalEvent],
           violation_fingerprint: ViolationFingerprint,
           _stats: MinimizationStats,
           init:Option[()=>Any]=None) : Option[EventTrace] = {
    stats = _stats
    Instrumenter().scheduler = this
    val tuple_option = explore(events, Some(violation_fingerprint))
    reset_all_state
    // test passes if we were unable to find a failure.
    tuple_option match {
      case Some((trace, violation)) =>
        return Some(trace)
      case None =>
        return None
    }
  }
}

// Our use of Uniq and Unique is somewhat confusing. Uniq is used to
// associate MsgSends with their subsequent MsgEvents.
// Unique is used by DepTracker.
//
// Inheritors must implement:
//   def iterator: Iterator[(Uniq[(Cell,Envelope)],Unique)]
//   def +=(elem: A): Growable.this.type
//   def clear(): Unit
// Plus these:
trait RandomizationStrategy extends
    Iterable[(Uniq[(Cell,Envelope)],Unique)] with
    Growable[(Uniq[(Cell,Envelope)],Unique)] {
  def removeRandomElement: (Uniq[(Cell,Envelope)],Unique)
  // Remove the first matching element
  def remove(snd: String, rcv: String, msg: Any)
  def removeAll(rcv: String): Seq[(Uniq[(Cell,Envelope)],Unique)]
}

class FullyRandom extends RandomizationStrategy {
  val pendingEvents = new RandomizedHashSet[Tuple2[Uniq[(Cell,Envelope)],Unique]]

  def iterator: Iterator[Tuple2[Uniq[(Cell,Envelope)],Unique]] =
    pendingEvents.iterator

  def +=(tuple: Tuple2[Uniq[(Cell,Envelope)],Unique]) : this.type = {
    pendingEvents += tuple
    return this
  }

  def clear(): Unit = {
    pendingEvents.clear
  }

  def remove(snd: String, rcv: String, msg: Any): Unit = {
    for (e <- pendingEvents.arr) {
      val otherSnd  = e._1._1.element._2.sender.path.name
      val otherRcv = e._1._1.element._1.self.path.name
      val otherMsg = e._1._1.element._2.message
      if (snd == otherSnd && rcv == otherRcv && msg == otherMsg) {
        pendingEvents.remove(e)
        return
      }
    }
  }

  def removeRandomElement(): (Uniq[(Cell,Envelope)],Unique) = {
    return pendingEvents.removeRandomElement
  }

  def removeAll(rcv: String): Seq[(Uniq[(Cell,Envelope)],Unique)] = {
    val result = new ListBuffer[(Uniq[(Cell,Envelope)],Unique)]
    for (e <- pendingEvents.arr) {
      val otherRcv = e._1._1.element._1.self.path.name
      if (rcv == otherRcv) {
        result += e._1
        pendingEvents.remove(e)
      }
    }
    return result
  }
}

// For each <src, dst> pair, maintain FIFO order. Other than that, fully
// random.
// TODO(cs): very strange behavior sometimes: NullPointerExceptions in
// removeAll, becuase srcDsts apparently contains null tuples. I am baffled as
// to why it happens, not even synchronized methods everywhere helps it. For
// now, keep the synchronized methods in...
class SrcDstFIFO extends RandomizationStrategy {
  private var srcDsts = new ArrayBuffer[(String, String)]
  private val rand = new Random(System.currentTimeMillis())
  private val srcDstToMessages = new HashMap[(String, String),
                                             Queue[(Uniq[(Cell,Envelope)],Unique)]]
  private val allMessages = new MultiSet[(Uniq[(Cell,Envelope)],Unique)]

  def getRandomSrcDst(): ((String, String), Int) = synchronized {
    val idx = rand.nextInt(srcDsts.length)
    return (srcDsts(idx), idx)
  }

  def iterator: Iterator[Tuple2[Uniq[(Cell,Envelope)],Unique]] = synchronized {
    // Hm, currently the only use of .iterator is .isEmpty. So it doesn't
    // really matter what order we give.
    allMessages.iterator
  }

  def +=(tuple: Tuple2[Uniq[(Cell,Envelope)],Unique]) : this.type = synchronized {
    val src = tuple._1.element._2.sender.path.name
    val dst = tuple._1.element._1.self.path.name
    if (!(srcDstToMessages contains ((src, dst)))) {
      // If there is no queue, create one
      assert(((src, dst)) != null)
      srcDsts += ((src, dst))
      srcDstToMessages((src, dst)) = new Queue[(Uniq[(Cell,Envelope)],Unique)]
    }

    // append to the queue
    srcDstToMessages((src, dst)) += tuple
    allMessages += tuple
    return this
  }

  def clear(): Unit = synchronized {
    srcDsts.clear()
    srcDstToMessages.clear()
    allMessages.clear()
  }

  def removeRandomElement(): (Uniq[(Cell,Envelope)],Unique) = synchronized {
    // First find a random queue. Then dequeue from the queue.
    val (srcDst, idx) = getRandomSrcDst
    val queue = srcDstToMessages(srcDst)
    val ret = queue.dequeue
    if (queue.isEmpty) {
      srcDstToMessages -= srcDst
      srcDsts.remove(idx)
    }
    allMessages -= ret
    return ret
  }

  def remove(src: String, dst: String, msg: Any): Unit = synchronized {
    if (!(srcDstToMessages contains ((src,dst)))) {
      return
    }
    val queue = srcDstToMessages((src,dst))
    queue.dequeueFirst(t => {
      val s = t._1.element._2.sender.path.name
      val d = t._1.element._1.self.path.name
      val m = t._1.element._2.message
      (s == src && d == dst && msg == m)
    }) match {
      case Some(t) =>
        allMessages -= t
        if (queue.isEmpty) {
          srcDstToMessages -= ((src,dst))
          srcDsts.remove(srcDsts.indexOf(((src,dst))))
        }
      case _ =>
    }
  }

  def removeAll(rcv: String): Seq[(Uniq[(Cell,Envelope)],Unique)] = synchronized {
    val result = new ListBuffer[(Uniq[(Cell,Envelope)],Unique)]
    srcDsts.toSeq.foreach {
      case (src, dst) =>
        if (dst == rcv) {
          val queue = srcDstToMessages((src, dst))
          for (e <- queue) {
            allMessages -= e
            result += e
          }
          srcDstToMessages -= ((src,rcv))
          srcDsts.remove(srcDsts.indexOf(((src,rcv))))
        }
      case null =>
        // WTF... How did we even get here...
        assert(!(srcDstToMessages contains null))
    }
    return result
  }
}
