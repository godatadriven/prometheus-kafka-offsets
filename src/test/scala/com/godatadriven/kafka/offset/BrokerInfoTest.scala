package com.godatadriven.kafka.offset

import com.google.gson.GsonBuilder
import org.apache.kafka.common.protocol.SecurityProtocol
import org.specs2.mutable.Specification

class BrokerInfoTest extends Specification {
  val gson = new GsonBuilder().create()

  "Brokerinfo host should" >> {
    "return host if host is filled" >> {
      val brokerInfo = gson.fromJson("{\"jmx_port\":-1,\"timestamp\":\"1459185255551\",\"host\":\"ron\",\"version\":1,\"port\":9092}", classOf[BrokerInfo])
      brokerInfo.getHost must equalTo("ron")
    }
    "return host if endpoinst is filled" >> {
      val brokerInfo = gson.fromJson("{\"jmx_port\":-1,\"timestamp\":\"1459185255551\",\"endpoints\": [\"PLAINTEXTSASL://ron:9092\"],\"host\":\"\",\"version\":1,\"port\":-1}", classOf[BrokerInfo])
      brokerInfo.getHost must equalTo("ron")
    }
  }

  "Brokerinfo port should" >> {
    "return port if port is filled" >> {
      val brokerInfo = gson.fromJson("{\"jmx_port\":-1,\"timestamp\":\"1459185255551\",\"host\":\"ron\",\"version\":1,\"port\":9092}", classOf[BrokerInfo])
      brokerInfo.getPort must equalTo(9092)
    }
    "return port if endpoinst is filled" >> {
      val brokerInfo = gson.fromJson("{\"jmx_port\":-1,\"timestamp\":\"1459185255551\",\"endpoints\": [\"PLAINTEXTSASL://ron:9092\"],\"host\":\"\",\"version\":1,\"port\":-1}", classOf[BrokerInfo])
      brokerInfo.getPort must equalTo(9092)
    }
  }

  "Brokerinfo securityProtocol should" >> {
    "return PLAINTEXT if no endpoint is not filled" >> {
      val brokerInfo = gson.fromJson("{\"jmx_port\":-1,\"timestamp\":\"1459185255551\",\"host\":\"ron\",\"version\":1,\"port\":9092}", classOf[BrokerInfo])
      brokerInfo.getSecurityProtocol must equalTo(SecurityProtocol.PLAINTEXT)
    }
    "return PLAINTEXTSASL if endpoinst is filled" >> {
      val brokerInfo = gson.fromJson("{\"jmx_port\":-1,\"timestamp\":\"1459185255551\",\"endpoints\": [\"PLAINTEXTSASL://ron:9092\"],\"host\":\"\",\"version\":1,\"port\":-1}", classOf[BrokerInfo])
      brokerInfo.getSecurityProtocol must equalTo(SecurityProtocol.PLAINTEXTSASL)
    }
//    "return SASL_PLAINTEXT if endpoinst is filled with this schema" >> {
//      val brokerInfo = gson.fromJson("{\"jmx_port\":-1,\"timestamp\":\"1459185255551\",\"endpoints\": [\"SASL_PLAINTEXT://ron:9092\"],\"host\":\"\",\"version\":1,\"port\":-1}", classOf[BrokerInfo])
//      brokerInfo.getSecurityProtocol must equalTo(SecurityProtocol.SASL_PLAINTEXT)
//    }
    "return PLAINTEXT if endpoinst is filled with this schema" >> {
      val brokerInfo = gson.fromJson("{\"jmx_port\":-1,\"timestamp\":\"1459185255551\",\"endpoints\": [\"PLAINTEXT://ron:9092\"],\"host\":\"\",\"version\":1,\"port\":-1}", classOf[BrokerInfo])
      brokerInfo.getSecurityProtocol must equalTo(SecurityProtocol.PLAINTEXT)
    }
  }
}
