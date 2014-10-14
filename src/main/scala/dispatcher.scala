package akka.dispatch

import java.util.concurrent.{ ConcurrentHashMap, TimeUnit, ThreadFactory }
import com.typesafe.config.{ ConfigFactory, Config }
import akka.actor.{ Scheduler, DynamicAccess, ActorSystem }
import akka.event.Logging.Warning
import akka.event.EventStream
import scala.concurrent.duration.{Duration, FiniteDuration}
import akka.ConfigurationException
import akka.actor.{Deploy, ActorCell}
import akka.util.Helpers.ConfigOps
import scala.concurrent.ExecutionContext
import akka.dispatch._

class InstrumentedDispatcher(
  _configurator: MessageDispatcherConfigurator,
  _id: String,
  throughput: Int,
  throughputDeadlineTime: Duration,
  _executorServiceFactoryProvider: ExecutorServiceFactoryProvider,
  _shutdownTimeout: FiniteDuration)
  extends Dispatcher(_configurator, _id, throughput, throughputDeadlineTime, _executorServiceFactoryProvider,
_shutdownTimeout) {
  protected[akka] override def dispatch(receiver: ActorCell, invocation: Envelope): Unit = {
    println("Dispatch called for " + receiver.actor.self.path.name + " " + invocation)
    super.dispatch(receiver, invocation)
    println("Done dispatching")
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

