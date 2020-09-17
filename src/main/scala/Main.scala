import EpdMain.{GracefulShutdown, StartSimulator}
import EpdManager.{StartAnalytics, StartMessages, StartRecords}
import akka.NotUsed
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.server.Directives.{complete, handleWebSocketMessages, path, pathEndOrSingleSlash}
import akka.stream.SinkShape
import akka.stream.scaladsl.{Broadcast, BroadcastHub, Flow, GraphDSL, Keep, MergeHub, Sink, Source}

import scala.concurrent.duration.DurationInt
import scala.io.StdIn
import scala.util.{Failure, Success}

object Main {

  def main(args: Array[String]) {

    implicit val system = ActorSystem(Behaviors.empty, "webtest")
    implicit val executionContext = system.executionContext

    system.log.info("Webtest Main started...")
    val cors = new CORSHandler {}

    val filePath = "/home/bw/"
    val equipFileName = "print02-34-sim"
    val epdConfig: EpdConfig = DataRecord.readConfigFile(filePath, equipFileName)

    val (hubSink, hubSource) =
      MergeHub.source[Message].toMat(BroadcastHub.sink[Message])(Keep.both).run()

    // all messages received from websockets will be processed by wsSink
    val messageSink: Sink[Message, NotUsed] = Sink.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val broadcaster = builder.add(Broadcast[Message](1))

      val wsSinkFlow: Flow[Message, Message, NotUsed] = Flow[Message].mapConcat {
        case tm: TextMessage =>
          val textMessage = TextMessage(Source.single("Hello ") ++ tm.textStream ++ Source.single("!")) :: Nil
          textMessage
        case bm: BinaryMessage =>
          // ignore binary messages but drain content to avoid the stream being clogged
          bm.dataStream.runWith(Sink.ignore)
          println("BinaryMessage received and forwarded...")
          Nil
      }

      // val webMessage: WebMessage = if(msg.isText) JsonUtil.toType[WebMessage](msg.asTextMessage.getStrictText) else Nil
      broadcaster.out(0) ~> wsSinkFlow ~> hubSink

      // expose port
      SinkShape(broadcaster.in)
    })

    def websocketFlow: Flow[Message, Message, Any] = Flow.fromSinkAndSource(messageSink, hubSource)

    Http().newServerAt("0.0.0.0", 8080)
      .bind(Directives.get {
        cors.corsHandler(
          Directives.concat(
            pathEndOrSingleSlash {
              println("Akka Http pathEnd | SingleSlash")
              complete(StatusCodes.OK)
            },
            path("ws") {
              println("Websocket Connection initiated...")
              handleWebSocketMessages(websocketFlow)
            }
          )
        )
      })
      .onComplete {
        case Success(binding) ⇒
          println(s"Server is listening on 0.0.0.0:8080")
          binding.addToCoordinatedShutdown(hardTerminationDeadline = 10.seconds)
        case Failure(e) ⇒
          println(s"Binding failed with ${e.getMessage}")
          system.terminate()
      }

    val epdMain: ActorRef[EpdMain.Command] = system.systemActorOf(EpdMain(epdConfig, hubSink), "EpdMain")
    epdMain ! StartSimulator

    system.log.info("Press RETURN to stop...")
    StdIn.readLine()
    epdMain ! GracefulShutdown
    system.terminate()
  }
}

object EpdMain {

  sealed trait Command
  case object StartSimulator extends Command
  case object GracefulShutdown extends Command

  def apply(epdConfig: EpdConfig, hubSink: Sink[Message, NotUsed]): Behavior[Command] = Behaviors
    .receive[Command] { (context, message) =>
      message match {
        case StartSimulator =>
          val epdManager = context.spawn(EpdManager(epdConfig, hubSink), "EpdManager")
          epdManager ! StartRecords
          epdManager ! StartAnalytics
          epdManager ! StartMessages
          Behaviors.same
        case GracefulShutdown =>
          Behaviors.stopped { () =>
            context.log.info("Initiating graceful shutdown in EpdMain...")
          }
      }
  }
}


