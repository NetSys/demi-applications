package akka.dispatch.verification

import akka.actor.ActorCell
import akka.actor.ActorSystem
import akka.actor.ActorRef
import akka.actor.Actor
import akka.actor.PoisonPill
import akka.actor.Props;

import akka.dispatch.Envelope
import akka.dispatch.MessageQueue
import akka.dispatch.MessageDispatcher

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.Iterator

// The current scheduler is changed from Vjeko's model: it records
// the set of events produced over time but does not actually replay
// anything at the moment (or rather what to replay is decided by some
// extension thereof).
class Scheduler {
  
  var intrumenter = Instrumenter
  var currentTime = 0
  var index = 0
  
  type CurrentTimeQueueT = Queue[Event]
  
  var currentlyProduced = new CurrentTimeQueueT
  var currentlyConsumed = new CurrentTimeQueueT
  
  var producedEvents = new Queue[ (Integer, CurrentTimeQueueT) ]
  var consumedEvents = new Queue[ (Integer, CurrentTimeQueueT) ]
  
  var prevProducedEvents = new Queue[ (Integer, CurrentTimeQueueT) ]
  var prevConsumedEvents = new Queue[ (Integer, CurrentTimeQueueT) ]
 
  // Current set of enabled events.
  val pendingEvents = new HashMap[String, Queue[(ActorCell, Envelope)]]  

  // Notification that the system has been reset
  def start_trace() : Unit = {
    prevProducedEvents = producedEvents
    prevConsumedEvents = consumedEvents
    producedEvents = new Queue[ (Integer, CurrentTimeQueueT) ]
    consumedEvents = new Queue[ (Integer, CurrentTimeQueueT) ]
  }

  // When executing a trace, find the next trace event
  def trace_iterator() : Option[Event] = { 
    if(prevConsumedEvents.isEmpty)
      return None
    val (count, q) = prevConsumedEvents.head
    q.isEmpty match {
      case true =>
        prevConsumedEvents.dequeue()
        trace_iterator()
      case false =>
        val ret = Some(q.dequeue())
        return ret
    }
  }
  
  // Get next event from the trace
  def get_next_trace_message() : Option[MsgEvent] = {
    trace_iterator() match {
      case Some(v : MsgEvent) =>  Some(v)
      case Some(v : Event) => get_next_trace_message()
      case None => None
    }
  }
  
  // Figure out what is the next message to schedule.
  def schedule_new_message() : Option[(ActorCell, Envelope)] = {
  
    // Filter for messages belong to a particular actor
    def extract(e: MsgEvent, c: (ActorCell, Envelope)) : Boolean = {
      val (cell, env) = c
      e.receiver == cell.self.path.name
    }

    // Get from the current set of pending events
    def get_pending_event()  : Option[(ActorCell, Envelope)] = {
      // Do we have some pending events
      pendingEvents.headOption match {
        case Some((receiver, queue)) =>
           if (queue.isEmpty == true) {
             pendingEvents.remove(receiver) match {
               case Some(key) => schedule_new_message()
               case None => throw new Exception("internal error")
             }
             
           } else {
              Some(queue.dequeue())
           }
        case None => None
      }
    }
    
    get_next_trace_message() match {
     // The trace says there is something to run.
     case Some(msg_event : MsgEvent) => 
       pendingEvents.get(msg_event.receiver) match {
         case Some(queue) => queue.dequeueFirst(extract(msg_event, _))
         case None => None
       }
     // The trace says there is nothing to run so we have either exhausted our
     // trace or are running for the first time. Use any enabled transitions.
     case None => get_pending_event()
       
   }
  }
  
  // Called by the scheduler or other entities to get next event
  def next_event() : Event = {
    trace_iterator() match {
      case Some(v) => v
      case None => throw new Exception("no previously consumed events")
    }
  }

  // Record that an event was consumed
  def event_consumed(event: Event) = {
    currentlyConsumed.enqueue(event)
  }
  
  def event_consumed(cell: ActorCell, envelope: Envelope) = {
    val value: (ActorCell, Envelope) = (cell, envelope)
    val receiver = cell.self
    val snd = envelope.sender.path.name
    val rcv = receiver.path.name
    currentlyConsumed.enqueue(new MsgEvent(snd, rcv, envelope.message, cell, envelope))
  }
  
  // Record that an event was produced 
  def event_produced(event: Event) = {
    currentlyProduced.enqueue(event)
  }
  
  def event_produced(cell: ActorCell, envelope: Envelope) = {
    val value: (ActorCell, Envelope) = (cell, envelope)
    val receiver = cell.self
    val snd = envelope.sender.path.name
    val rcv = receiver.path.name
    val msgs = pendingEvents.getOrElse(rcv, new Queue[(ActorCell, Envelope)])
    pendingEvents(rcv) = msgs += ((cell, envelope))
    currentlyProduced.enqueue(new MsgEvent(snd, rcv, envelope.message, cell, envelope))
  }
  
  def event_produced(_parent: String,
    _props: Props, _name: String, _actor: ActorRef) = {
    currentlyProduced.enqueue(new SpawnEvent(_parent, _props, _name, _actor))
  }
  
  // Called before we start processing a newly received event
  def before_receive(cell: ActorCell) {
    producedEvents.enqueue( (currentTime, currentlyProduced) )
    consumedEvents.enqueue( (currentTime, currentlyConsumed) )
    currentlyProduced = new CurrentTimeQueueT
    currentlyConsumed = new CurrentTimeQueueT
    currentTime += 1
  }
  
  // Called after receive is done being processed 
  def after_receive(cell: ActorCell) {
  }

}
