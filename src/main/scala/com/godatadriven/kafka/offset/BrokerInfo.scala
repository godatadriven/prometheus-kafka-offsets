package com.godatadriven.kafka.offset

import java.net.{URI, URISyntaxException}

class BrokerInfo(private[this] val endpoints: Array[String],
                 private[this] val host: String,
                 private[this] val port: Int) {

  private[this] def firstEndpointUri: Option[URI] = {
    if (endpoints == null) {
      return None
    }
    endpoints.headOption.flatMap { endpoint =>
      try {
        Some(new URI(endpoint))
      } catch {
        case e: URISyntaxException => None
      }
    }
  }

  def getHost: String = {
    if (host == null || host.isEmpty) {
      firstEndpointUri.head.getHost
    } else {
      host
    }
  }

  def getPort: Int = {
    if (port == -1) {
      firstEndpointUri.head.getPort
    } else {
      port
    }
  }

}
