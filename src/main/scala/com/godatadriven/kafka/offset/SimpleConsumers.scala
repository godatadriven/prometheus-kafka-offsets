package com.godatadriven.kafka.offset

import com.google.gson.{Gson, GsonBuilder}
import kafka.consumer.SimpleConsumer
import org.apache.zookeeper.ZooKeeper

import scala.collection.JavaConversions._

class SimpleConsumers(zookeeper: ZooKeeper) {
  val gson: Gson = new GsonBuilder().create()

  val children: Map[String, SimpleConsumer] = zookeeper.getChildren("/brokers/ids", false).map(id => {
    val brokerInfoJson: String = new String(zookeeper.getData("/brokers/ids/" + id, false, null))

    val brokerInfo = gson.fromJson(brokerInfoJson, classOf[BrokerInfo])
    id -> new SimpleConsumer(brokerInfo.getHost, brokerInfo.getPort, 10000, 100000, "consumerOffsetChecker")
  }).toMap

  def get(key: String): Option[SimpleConsumer] = {
    children.get(key)
  }

  def close(): Unit = {
    children.foreach(_._2.close())
  }
}
