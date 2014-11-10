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
import scala.collection.mutable.Stack
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.util.control.Breaks

class Event

class MsgEvent(_sender: String, _receiver: String, _msg: Any, 
               _cell: ActorCell, _envelope: Envelope) extends Event {
  
  val sender = _sender
  val receiver = _receiver
  val msg = _msg
  val cell = _cell
  val envelope = _envelope
}


class SpawnEvent(_parent: String,
    _props: Props, _name: String, _actor: ActorRef) extends Event {
  
  val parent = _parent
  val props = _props
  val name = _name
  val actor = _actor
}





class Instrumenter {

  val scheduler = new Scheduler(this)
  val dispatchers = new HashMap[ActorRef, MessageDispatcher]
  
  val allowedEvents = new HashSet[(ActorCell, Envelope)]  
  
  val seenActors = new HashSet[(ActorSystem, Any)]
  val actorMappings = new HashMap[String, ActorRef]
  val actorNames = new HashSet[String]
  
  var currentActor = ""
  var counter = 0   
  var started = false;
  
  
  def new_actor(system: ActorSystem, 
      props: Props, name: String, actor: ActorRef) = {
    
    println("System has created a new actor: " + actor.path.name)
    
    val event = new SpawnEvent(currentActor, props, name, actor)
    scheduler.event_produced(event)
    scheduler.event_consumed(event)
    
    if (!started) {
      seenActors += ((system, (actor, props, name)))
    }
    
    actorMappings(name) = actor
    actorNames += name
  }
  
  def new_actor(system: ActorSystem, 
      props: Props, actor: ActorRef) = {
    
    println("System has created a new actor: " + actor.path.name)
    
    val event = new SpawnEvent(currentActor, props, actor.path.name, actor)
    scheduler.event_produced(event)
    scheduler.event_consumed(event)

    if (started) {
      seenActors += ((system, (actor, props)))
    }
  }
  
  
  
  def restart_system(sys: ActorSystem, argQueue: Queue[Any]) {
    
    val newSystem = ActorSystem("new-system-" + counter)
    counter += 1
    println("Started a new actor system.")

    scheduler.start_trace()
    
    val first_spawn = scheduler.next_event() match {
      case e: SpawnEvent => e
      case _ => throw new Exception("not a spawn")
    }
    
    
    for (args <- argQueue) {
      args match {
        case (actor: ActorRef, props: Props, first_spawn.name) =>
          println("starting " + first_spawn.name)
          newSystem.actorOf(props, first_spawn.name)
      }
    }

    
    val first_msg = scheduler.next_event() match {
      case e: MsgEvent => e
      case _ => throw new Exception("not a message")
    }
    
    actorMappings.get(first_msg.receiver) match {
      case Some(ref) => ref ! first_msg.msg
      case None => throw new Exception("no such actor " + first_msg.receiver)
    }
  }
  
  
  
  def trace_finished() = {
    println("Done executing the trace.")
    started = false
    scheduler.trace_finished()
    
    val allSystems = new HashMap[ActorSystem, Queue[Any]]
    for ((system, args) <- seenActors) {
      val argQueue = allSystems.getOrElse(system, new Queue[Any])
      argQueue.enqueue(args)
      allSystems(system) = argQueue
    }

    seenActors.clear()
    for ((system, argQueue) <- allSystems) {
        println("Shutting down the actor system. " + argQueue.size)
        system.shutdown()
        system.registerOnTermination(restart_system(system, argQueue))
    }
  }
  
  
  def beforeMessageReceive(cell: ActorCell) {
    
    if (isSystemMessage(cell.sender.path.name, cell.self.path.name)) return

    scheduler.before_receive(cell)
    currentActor = cell.self.path.name
    
    println(Console.GREEN 
        + " ↓↓↓↓↓↓↓↓↓ ⌚  " + scheduler.currentTime + " | " + cell.self.path.name + " ↓↓↓↓↓↓↓↓↓ " + 
        Console.RESET)
  }
  

  def afterMessageReceive(cell: ActorCell) {
    if (isSystemMessage(cell.sender.path.name, cell.self.path.name)) return
    println(Console.RED 
        + " ↑↑↑↑↑↑↑↑↑ ⌚  " + scheduler.currentTime + " | " + cell.self.path.name + " ↑↑↑↑↑↑↑↑↑ " 
        + Console.RESET)
        
    scheduler.after_receive(cell)          
    scheduler.schedule_new_message() match {
      case Some((new_cell, envelope)) => dispatch_new_message(new_cell, envelope)
      case None =>
        counter += 1
        if (counter < 4) trace_finished()
        else println("Done.")
    }
  }
  
  
  
  
  

  def dispatch_new_message(cell: ActorCell, envelope: Envelope) = {
    val snd = envelope.sender.path.name
    val rcv = cell.self.path.name
    
    allowedEvents += ((cell, envelope) : (ActorCell, Envelope))        

    val dispatcher = dispatchers.get(cell.self) match {
      case Some(value) => value
      case None => throw new Exception("internal error")
    }
    
    scheduler.event_consumed(cell, envelope)
    dispatcher.dispatch(cell, envelope)
  }
  
  
  
  def isSystemMessage(src: String, dst: String): Boolean = {

    if ((actorNames contains src) ||
        (actorNames contains dst)
    ) return false
    
    return true
  }
  
  
  
  def aroundDispatch(dispatcher: MessageDispatcher, cell: ActorCell, 
      envelope: Envelope): Boolean = {
    
    val value: (ActorCell, Envelope) = (cell, envelope)
    val receiver = cell.self
    val snd = envelope.sender.path.name
    val rcv = receiver.path.name
    
    if (isSystemMessage(snd, rcv)) { return true }
    
    if (allowedEvents contains value) {
      allowedEvents.remove(value) match {
        case true => 
          return true
        case false => throw new Exception("internal error")
      }
    }
    
    dispatchers(receiver) = dispatcher
    if (!started) {
      started = true
      dispatch_new_message(cell, envelope)
      return false
    }
    
    scheduler.event_produced(cell, envelope)
    
    println(Console.BLUE + "enqueue: " + snd + " -> " + rcv + Console.RESET);

    return false
  }

}

