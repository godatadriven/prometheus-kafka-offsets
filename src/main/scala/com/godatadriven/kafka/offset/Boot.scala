package com.godatadriven.kafka.offset

import akka.actor.{ActorSystem, Props}
import akka.io.IO
import spray.can.Http
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._

object Boot extends App {
  val thread = new Thread {
    override def run {
      KafkaOffsetConsumer.run()
    }
  }
  thread.start

  // we need an ActorSystem to host our application in
  implicit val system = ActorSystem("on-spray-can")

  private[this] val config = system.settings.config
  val port = config.getInt("service.http.port")
  val interface = config.getString("service.http.bind")

  // create and start our service actor
  val service = system.actorOf(Props[RestServiceActor], "demo-service")

  implicit val timeout = Timeout(5.seconds)
  // start a new HTTP server on port 8080 with our service actor as the handler
  IO(Http) ? Http.Bind(service, interface = interface, port = port)
}