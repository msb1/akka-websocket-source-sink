import akka.NotUsed
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.http.scaladsl.model.ws.Message
import akka.stream.scaladsl.Sink

import scala.concurrent.duration.DurationInt

object EpdManager {

  sealed trait Simulator

  case object StartRecords extends Simulator

  case object StartAnalytics extends Simulator

  case object StartMessages extends Simulator

  private case object Records extends Simulator

  private case object Analytics extends Simulator

  private case object Messages extends Simulator

  private case object RecordTimerKey

  private case object AnalyticTimerKey

  private case object MessageTimerKey

  def apply(epdConfig: EpdConfig, hubSink: Sink[Message, NotUsed]): Behavior[Simulator] =
    Behaviors.setup(context => {
      context.log.info("EPD Manager Object started...")
      new EpdManager(context, epdConfig, hubSink)
    })
}


class EpdManager(context: ActorContext[EpdManager.Simulator], epdConfig: EpdConfig, hubSink: Sink[Message, NotUsed])
  extends AbstractBehavior[EpdManager.Simulator](context) {

  import EpdManager._
  import GenActor._

  context.log.info("EPD Manager started...")
  val genRecord = context.spawn(GenActor(epdConfig, hubSink), "GenActor")

  override def onMessage(msg: Simulator): Behavior[Simulator] =
    msg match {

      case StartRecords => {
        Behaviors.withTimers[Simulator] { timers =>
          timers.startTimerWithFixedDelay(RecordTimerKey, Records, 2000.millisecond)
          this
        }
      }
      case Records =>
        genRecord ! GenRecord
        this

      case StartAnalytics => {
        Behaviors.withTimers[Simulator] { timers =>
          timers.startTimerWithFixedDelay(AnalyticTimerKey, Analytics, 2500.millisecond)
          this
        }
      }
      case Analytics =>
        genRecord ! GenAnalytic
        this

      case StartMessages => {
        Behaviors.withTimers[Simulator] { timers =>
          timers.startTimerWithFixedDelay(MessageTimerKey, Messages, 5000.millisecond)
          this
        }
      }
      case Messages =>
        genRecord ! GenMessage
        this

      case _ => Behaviors.empty
    }
}




