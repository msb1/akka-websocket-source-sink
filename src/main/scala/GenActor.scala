import java.text.SimpleDateFormat
import java.util.{Calendar, UUID}

import akka.NotUsed
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.stream.scaladsl.{Sink, Source}

case class WebMessage(msgType: String, msgId: String, msgTime: String, user: String, value: String)

object WebEvent {
  val CHAT_MESSAGE = "CHAT_MESSAGE"
  val DATA_MESSAGE = "DATA_MESSAGE"
  val ANALYTIC_MESSAGE = "ANALYTIC_MESSAGE"
  val USER_JOINED = "USER_JOINED"
  val USER_LEFT = "USER_LEFT"
}

object GenActor {

  sealed trait Generator

  case object GenMessage extends Generator

  case object GenRecord extends Generator

  case object GenAnalytic extends Generator

  case object Terminate extends Generator

  def apply(epdConfig: EpdConfig, hubSink: Sink[Message, NotUsed]):
  Behavior[Generator] = Behaviors.setup(context => new GenActor(context, epdConfig, hubSink))

}

class GenActor(context: ActorContext[GenActor.Generator], epdConfig: EpdConfig, hubSink: Sink[Message, NotUsed]) extends AbstractBehavior[GenActor.Generator](context) {

  import GenActor._

  implicit val system = context.system
  context.log.info("Gen Actor started...")
  private val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  override def onMessage(msg: GenActor.Generator): Behavior[GenActor.Generator] =
    msg match {
      case GenMessage => {
        val uuid = UUID.randomUUID().toString
        val webMessage = WebMessage(WebEvent.CHAT_MESSAGE, uuid, timeFormat.format(Calendar.getInstance.getTime), "TopTech", s"CHAT_MESSAGE from WebTest: UUID=$uuid")
        val webMessageString = JsonUtil.toString[WebMessage](webMessage)
        context.log.info(webMessageString)
        Source.single(TextMessage(webMessageString)).runWith(hubSink)
        this
      }
      case GenRecord => {
        val dataRecord = DataRecord.makeSimulatedRecord(epdConfig)
        val uuid = UUID.randomUUID().toString
        val webMessage = WebMessage(WebEvent.DATA_MESSAGE, uuid, timeFormat.format(Calendar.getInstance.getTime), "TopTech", dataRecord)
        val webMessageString = JsonUtil.toString[WebMessage](webMessage)
        context.log.info(webMessageString)
        Source.single(TextMessage(webMessageString)).runWith(hubSink)
        this
      }
      case GenAnalytic => {
        val dataRecord = DataRecord.makeSimulatedAnalytic(epdConfig)
        val uuid = UUID.randomUUID().toString
        val webMessage = WebMessage(WebEvent.ANALYTIC_MESSAGE, uuid, timeFormat.format(Calendar.getInstance.getTime), "TopTech", dataRecord)
        val webMessageString = JsonUtil.toString[WebMessage](webMessage)
        context.log.info(webMessageString)
        Source.single(TextMessage(webMessageString)).runWith(hubSink)
        this
      }
      case Terminate => {
        val uuid = UUID.randomUUID().toString
        val webMessage = WebMessage(WebEvent.CHAT_MESSAGE, uuid, timeFormat.format(Calendar.getInstance.getTime), "TopTech", "DATA DISCONNECTED")
        Source.single(TextMessage(JsonUtil.toString[WebMessage](webMessage))).runWith(hubSink)
        context.log.info("System terminate for Record Generator...")
        this
      }
    }
}

