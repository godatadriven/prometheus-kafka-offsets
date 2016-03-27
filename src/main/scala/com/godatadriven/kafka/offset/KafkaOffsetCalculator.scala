package com.godatadriven.kafka.offset

import java.util

import com.google.gson.GsonBuilder
import kafka.api.{OffsetRequest, OffsetResponse, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.zookeeper.KeeperException.NoNodeException
import org.apache.zookeeper.ZooKeeper

import scala.collection.JavaConversions._
import scala.collection.{immutable, mutable}

class KafkaOffsetCalculator {
  val SECURITYPROTOCOL = SecurityProtocol.PLAINTEXT
  val ZOOKEEPER_URL = "localhost:2181"

  val gson = new GsonBuilder().create()

  def main(args: Array[String]) {
    println(getTopicOffset())
  }

  def getTopicOffset(): String = {
    var result = new StringBuilder

    val zookeeper: ZooKeeper = new ZooKeeper(ZOOKEEPER_URL, 10000, null, true)
    val simpleConsumers = createConsumers(zookeeper)
    val topicsConsumers = getTopicConsumers(zookeeper)
    topicsConsumers.foreach(topic => {
      topic._2.foreach(topicConsumer => {
        getPartitionsAndLeaders(zookeeper, topicConsumer._1, simpleConsumers).foreach(partitionsAndLeader => {
          val consumerId: String = topicConsumer._2

          val topicAndPartition: TopicAndPartition = TopicAndPartition(topicConsumer._1, partitionsAndLeader._1.toInt)
          val offsetResponse: OffsetResponse = partitionsAndLeader._2.getOffsetsBefore(OffsetRequest(immutable.Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1))))
          val logSize: Long = offsetResponse.partitionErrorAndOffsets(topicAndPartition).offsets.head
          val offset: Long = getConsumerOffset(zookeeper, consumerId, topicConsumer._1, partitionsAndLeader._1)
          result ++= "topic: %s group: %s, partition %s logSize: %d offset: %d, lag: %d\n".format(topicConsumer._1, consumerId, partitionsAndLeader._1, logSize, offset, logSize - offset)
        })
      })
    })
//    result ++= "\n"
    result.toString
  }

  def getTopicConsumers(zookeeper: ZooKeeper): Map[String, mutable.Buffer[(String, String)]] = {
    getConsumers(zookeeper).flatMap(consumer => {
      getTopics(zookeeper, consumer)
    }).groupBy(_._1)
  }

  def getConsumers(zookeeper: ZooKeeper): util.List[String] = {
    zookeeper.getChildren("/consumers", false)
  }

  def getTopics(zookeeper: ZooKeeper, consumer: String): mutable.Buffer[(String, String)] = {
    try {
      return zookeeper.getChildren("/consumers/%s/offsets".format(consumer), false).map((_, consumer))
    } catch {
      case e: NoNodeException =>
        mutable.Buffer.empty
    }
  }

  def getConsumerOffset(zookeeper: ZooKeeper, group_id: String, topic: String, partition: String): Long = {
    try {
      new String(zookeeper.getData("/consumers/%s/offsets/%s/%s".format(group_id, topic, partition), false, null)).toLong
    } catch {
      case e: NoNodeException =>
        0L
    }
  }

  def getPartitionsAndLeaders(zookeeper: ZooKeeper, topic: String, consumers: Map[String, SimpleConsumer]): mutable.Buffer[(String, SimpleConsumer)] = {
    val children: util.List[String] = zookeeper.getChildren("/brokers/topics/%s/partitions".format(topic), false)
    children.map(id => {
      val partitionJson: String = new String(zookeeper.getData("/brokers/topics/%s/partitions/%s/state".format(topic, id), false, null))
      val partitionInfo = gson.fromJson(partitionJson, classOf[PartitionInfo])
      (id -> consumers.get(partitionInfo.leader).get)
    })
  }

  def createConsumers(zookeeper: ZooKeeper): Map[String, SimpleConsumer] = {
    val children: util.List[String] = zookeeper.getChildren("/brokers/ids", false)
    children.map(id => {
      val brokerInfoJson: String = new String(zookeeper.getData("/brokers/ids/" + id, false, null))

      val brokerInfo = gson.fromJson(brokerInfoJson, classOf[BrokerInfo])
      (id -> new SimpleConsumer(brokerInfo.host, brokerInfo.port, 10000, 100000, "consumerOffsetChecker", SECURITYPROTOCOL))
    }).toMap
  }
}