package akka.dispatch.verification

// Shared functionality for Scheduler subtypes.
// TODO(cs):  if it turns out that there isn't any shared functionality beyond what we currently have, it isn't worth
// the added complexity to have this supertype.
abstract class AbstractScheduler extends Scheduler {
  def isSystemMessage(src: String, dst: String): Boolean = {
    return dst == AuxiliaryActors.logSinkName
  }
}


