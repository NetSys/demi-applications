package runner.raftserialization

import pl.project13.scala.akka.raft.example._
import pl.project13.scala.akka.raft.protocol._
import pl.project13.scala.akka.raft.example.protocol._
import pl.project13.scala.akka.raft._
import pl.project13.scala.akka.raft.model._
import akka.dispatch.verification.{JavaSerialization, MessageSerializer, MessageDeserializer, Util}
import akka.actor.{ActorSystem, ExtendedActorSystem, ActorRef}
import akka.serialization._

import scala.collection.immutable.Seq

import java.nio._

class RaftMessageDeserializer(actorSystem: ActorSystem) extends MessageDeserializer {
  val extended = actorSystem.asInstanceOf[ExtendedActorSystem]

  // N.B. for now, no client message can have any sender other than
  // "deadLetters"
  def deserialize(buffer: ByteBuffer): Any = {
    val msg = JavaSerialization.deserialize[Any](buffer)
    // Kinda cludgy...
    if (msg.isInstanceOf[Set[_]]) {
      val set = msg.asInstanceOf[Set[String]]
      return ChangeConfiguration(ClusterConfiguration(set.map(m =>
        actorSystem.actorFor("/user/" + m)
      )))
    }
    msg match {
      case AppendWord(word) =>
        return ClientMessage[AppendWord](
          actorSystem.deadLetters, msg.asInstanceOf[AppendWord])
      case _ =>
        return msg
    }
  }
}

class RaftMessageSerializer extends MessageSerializer {
  def serialize(msg: Any): ByteBuffer = {
    // Cludgy that we have to deal with ClusterConfigurations this way..
    msg match {
      case ChangeConfiguration(StableClusterConfiguration(_, members)) =>
        val set = members.map(m =>
          m.path.name
        )
        return JavaSerialization.serialize(set)
      case ClientMessage(client, cmd) =>
        return JavaSerialization.serialize(cmd)
      case _ =>
        return JavaSerialization.serialize(msg)
    }
  }
}

