package com.godatadriven.kafka.offset

import java.net.{URI, URISyntaxException}
import java.util.Locale

import org.apache.kafka.common.protocol.SecurityProtocol

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

  def securityProtocol: SecurityProtocol = {
    firstEndpointUri match {
      case Some(uri) =>
        val scheme = uri.getScheme.toUpperCase(Locale.ROOT)
        if (scheme == SecurityProtocol.PLAINTEXTSASL.name)
          SecurityProtocol.PLAINTEXTSASL
        else if (scheme == SecurityProtocol.SASL_PLAINTEXT.name)
          SecurityProtocol.SASL_PLAINTEXT
        else
          SecurityProtocol.PLAINTEXT
      case None => SecurityProtocol.PLAINTEXT
    }
  }
}
