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

import scala.collection.generic.GenericTraversableTemplate

// Just a very simple, non-null scheduler
class TemplateScheduler extends Scheduler {
  
  var intrumenter = Instrumenter
  var currentTime = 0
  var index = 0
  
  type CurrentTimeQueueT = Queue[Event]
  
  // Current set of enabled events.
  val pendingEvents = new HashMap[String, Queue[(ActorCell, Envelope)]]  

  val actorNames = new HashSet[String]
  val actorQueue = new Queue[String]
  
  
  // Is this message a system message
  def isSystemMessage(src: String, dst: String): Boolean = {
    if ((actorNames contains src) || (actorNames contains dst))
      return false
    
    return true
  }
  
  def nextActor () : String = {
    val next = actorQueue(index)
    println("Actor queue = " + actorQueue + " actorNames = " + actorNames + " index = " + index)
    index = (index + 1) % actorQueue.size
    next
  }
  
  // Notification that the system has been reset
  def start_trace() : Unit = {
  }
  
  
  // Figure out what is the next message to schedule.
  def schedule_new_message() : Option[(ActorCell, Envelope)] = {
    val receiver = nextActor()
    // Do we have some pending events
    if (pendingEvents.isEmpty) {
      None
    } else {
      println("Active actors = " + pendingEvents.keys + " receiver = " + receiver)
    }

    pendingEvents.get(receiver) match {
      case Some(queue) =>
        if (queue.isEmpty == true) {
          pendingEvents.remove(receiver) match {
            case Some(key) => schedule_new_message()
            case None => throw new Exception("Internal error")
          }
        } else {
          Some(queue.dequeue())
        }
      case None =>
        if (pendingEvents.isEmpty) {
          None
        } else {
           schedule_new_message()
        }
    }
  }
  
  
  // Get next event
  def next_event() : Event = {
    throw new Exception("NYI")
  }
  

  // Record that an event was consumed
  def event_consumed(event: Event) = {
  }
  
  
  def event_consumed(cell: ActorCell, envelope: Envelope) = {
  }
  
  // Record that an event was produced 
  def event_produced(event: Event) = {
    event match {
      case event : SpawnEvent => 
        println("Spawned a new actor " + event.name)
        if (!(actorNames contains event.name)) {
          actorQueue += event.name
          actorNames += event.name
        }
    }
  }
  
  def event_produced(cell: ActorCell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    val msgs = pendingEvents.getOrElse(rcv, new Queue[(ActorCell, Envelope)])
    
    pendingEvents(rcv) = msgs += ((cell, envelope))
  }
  
  // Called before we start processing a newly received event
  def before_receive(cell: ActorCell) {
    currentTime += 1
    println(Console.GREEN 
        + " ↓↓↓↓↓↓↓↓↓ ⌚  " + currentTime + " | " + cell.self.path.name + " ↓↓↓↓↓↓↓↓↓ " + 
        Console.RESET)
  }
  
  // Called after receive is done being processed 
  def after_receive(cell: ActorCell) {
    println(Console.RED 
        + " ↑↑↑↑↑↑↑↑↑ ⌚  " + currentTime + " | " + cell.self.path.name + " ↑↑↑↑↑↑↑↑↑ " 
        + Console.RESET)
        
  }
  
  def notify_quiescence () {
  }
}
