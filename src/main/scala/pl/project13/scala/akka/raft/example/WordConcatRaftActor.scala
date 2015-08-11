package pl.project13.scala.akka.raft.example

import protocol._
import pl.project13.scala.akka.raft._
import pl.project13.scala.akka.raft.model._
import pl.project13.scala.akka.raft.protocol._

import akka.dispatch.verification.{CheckpointRequest, CheckpointReply, CheckpointSink, Instrumenter}

import scala.collection.mutable.ListBuffer
import pl.project13.scala.akka.raft.RaftActor

class WordConcatRaftActor extends RaftActor {

  type Command = Cmnd

  var words = Vector[String]()

  /** Called when a command is determined by Raft to be safe to apply */
  def apply = {
    case AppendWord(word) =>
      words = words :+ word
      log.info(s"Applied command [AppendWord($word)], full words is: $words")

      word

    case GetWords =>
      log.info("Replying with {}", words.toList)
      words.toList
  }

  // Deal with CheckpointRequests, for checking global invariants.
  override def receive = {
    case CheckpointRequest =>
      val state = List(replicatedLog, nextIndex, matchIndex, stateData, words)
      context.actorFor("../" + CheckpointSink.name) ! CheckpointReply(state)
    case m =>
      //println("RAFT " + self.path.name + " FSM received " + m + " " + super.getLog.map(_.stateName) + " " +
      //  isTimerActive(ElectionTimeoutTimerName) )
      println("BEFORE RECEIVE, LOG: " + replicatedLog)
      println("BEFORE RECEIVE, STATE: " + stateData)
      println("BEFORE RECEIVE, words: " + words)
      println("BEFORE RECEIVE, nextIndex: " + nextIndex)
      println("BEFORE RECEIVE, matchIndex: " + matchIndex)
      super.receive(m)
      println("AFTER RECEIVE, LOG: " + replicatedLog)
      println("AFTER RECEIVE, STATE: " + stateData)
      println("AFTER RECEIVE, words: " + words)
      println("AFTER RECEIVE, nextIndex: " + nextIndex)
      println("AFTER RECEIVE, matchIndex: " + matchIndex)
      //println("RAFT " + self.path.name + " Done FSM received " + m + " " + super.getLog.map(_.stateName))
  }

}

