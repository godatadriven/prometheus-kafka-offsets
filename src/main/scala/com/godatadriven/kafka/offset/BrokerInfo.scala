package com.godatadriven.kafka.offset

import java.net.{URI, URISyntaxException}

import org.apache.kafka.common.protocol.SecurityProtocol

class BrokerInfo(endpointsp: Array[String], hostp: String, portp: Int) {
  val endpoints: Array[String] = endpointsp
  val host: String = hostp
  val port: Int = portp

  def getHost() : String = {
    if (host != null && !host.isEmpty()) {
      return host
    } else if (endpoints != null && endpoints.length > 0) {
      try {
        val uri = new URI(endpoints(0))
        return uri.getHost()
      } catch {
        case e: URISyntaxException =>
      }
    }
    return null
  }

  def getPort() : Int = {
    if (port != -1) {
      return port
    } else if (endpoints != null && endpoints.length > 0) {
      try {
        val uri = new URI(endpoints(0))
        return uri.getPort()
      } catch {
        case e: URISyntaxException =>
      }
    }
    return -1
  }

  def getSecurityProtocol() : SecurityProtocol = {
    if (endpoints != null && endpoints.length > 0) {
      try {
        val uri = new URI(endpoints(0))
        if (uri.getScheme().toUpperCase().equals(SecurityProtocol.PLAINTEXTSASL.name)) {
          return SecurityProtocol.PLAINTEXTSASL
        } else if (uri.getScheme().toUpperCase().equals(SecurityProtocol.SASL_PLAINTEXT.name)) {
          return SecurityProtocol.SASL_PLAINTEXT
        }
      } catch {
        case e: URISyntaxException =>
      }
    }
    return SecurityProtocol.PLAINTEXT
  }
}
