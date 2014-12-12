package akka.dispatch.verification;

import akka.cluster.VectorClock
import scala.util.parsing.json.JSONObject

// TODO(cs): move the failure detector to this file.

import akka.actor.{Actor, ActorCell, ActorRef, ActorSystem, Props}

class LogSink () extends Actor {
  var actor2vc : Map[String, VectorClock] = Map()

  // TODO(cs): is there a way to specify default values for Maps in scala?
  def ensureKeyExists(key: String) : VectorClock = {
    if (!actor2vc.contains(key)) {
      actor2vc = actor2vc + (key -> new VectorClock())
    }
    return actor2vc(key)
  }

  def receive = {
    case LogMessage(msg) =>
      val senderName = sender().path.name
      val vc = ensureKeyExists(senderName)
      // Increment the clock.
      vc :+ senderName
      // Then print it, along with the message.
      println(JSONObject(vc.versions).toString() + " " + senderName + ": " + msg)
    case MergeVectorClocks(src, dst) =>
      val srcVC = ensureKeyExists(src)
      var dstVC = ensureKeyExists(dst)
      // Increment, then merge the clocks.
      dstVC :+ dst
      dstVC = dstVC.merge(srcVC)
      actor2vc = actor2vc + (dst -> dstVC)
  }
}

object AuxiliaryActors {
  val logSinkName = "log_sink"

  var instrumenter = Instrumenter()
  instrumenter.actorSystem.actorOf(Props[LogSink], logSinkName)
}


