package akka.dispatch

import com.typesafe.config.{ ConfigFactory, Config }
import akka.event.Logging.Warning
import akka.event.EventStream
import scala.concurrent.duration.{Duration, FiniteDuration, MINUTES, SECONDS}
import akka.ConfigurationException
import akka.actor.{Deploy, ActorCell, Cell, ActorPath}
import akka.util.Helpers.ConfigOps
import scala.concurrent.ExecutionContext
import scala.concurrent.forkjoin.{ ForkJoinTask, ForkJoinPool }
import scala.collection.mutable.ListBuffer

class InstrumentedDispatcher(
  _configurator: MessageDispatcherConfigurator,
  _id: String,
  throughput: Int,
  throughputDeadlineTime: Duration,
  _executorServiceFactoryProvider: ExecutorServiceFactoryProvider,
  _shutdownTimeout: FiniteDuration)
  extends Dispatcher(_configurator, _id, throughput, throughputDeadlineTime, _executorServiceFactoryProvider,
_shutdownTimeout) {
  val agents = new ListBuffer[ActorPath]
  val sched = new ListBuffer[String]
  protected[akka] override def dispatch(receiver: ActorCell, invocation: Envelope): Unit = {
    if (receiver.self != null && receiver.self.path != null) {
      if (invocation.sender != null && invocation.sender.path != null) {
        sched += (receiver.self.path.name + " " + invocation.sender.path.name)
      } else {
        sched += (receiver.self.path.name + " ")
      }
    }
    super.dispatch(receiver, invocation)
  }
  
  protected[akka] override def createMailbox(actor: Cell, mailboxType: MailboxType): Mailbox = {
    val act = actor.self
    if (act != null && act.path != null) {
      agents += act.path
    } 
    super.createMailbox(actor, mailboxType)
  }

  def awaitQuiscence() : Boolean = {
    return super.executorService.executor.asInstanceOf[ForkJoinPool].awaitQuiescence(5, MINUTES)
  }
}

class InstrumentedDispatcherConfigurator(config: Config, prerequisites: DispatcherPrerequisites)
  extends MessageDispatcherConfigurator(config, prerequisites) {

  private val instance = new InstrumentedDispatcher(this,
    "instrument.dispatcher",
    config.getInt("throughput"),
    config.getNanosDuration("throughput-deadline-time"),
    configureExecutor(),
    config.getMillisDuration("shutdown-timeout"))
  override def dispatcher(): MessageDispatcher = instance
}
