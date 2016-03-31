package com.godatadriven.kafka.offset

import java.util

import com.google.gson.GsonBuilder
import kafka.api.{OffsetRequest, OffsetResponse, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import org.apache.zookeeper.KeeperException.NoNodeException
import org.apache.zookeeper.ZooKeeper

import scala.collection.JavaConversions._
import scala.collection.{immutable, mutable}

case class PartitionInfo(leader: String)

object KafkaOffsetCalculator {
  val gson = new GsonBuilder().create()
  val zookeeperUrl = System.getProperty("zookeeper_url", "localhost:2181")

  def main(args: Array[String]) {
    println(getTopicOffset)
  }

  def closeSimpleConsumers(simpleConsumers: Map[String, SimpleConsumer]) =  {
    simpleConsumers.foreach(_._2.close())
  }

  def getTopicOffset: String = {
    var result = new StringBuilder

    val zookeeper: ZooKeeper = new ZooKeeper(zookeeperUrl, 10000, null, true)
    val simpleConsumers = createConsumers(zookeeper)
    val topicsConsumers = getTopicConsumers(zookeeper)
    topicsConsumers.foreach(topic => {
      val partitionsAndLeaders: mutable.Buffer[(String, SimpleConsumer)] = getPartitionsAndLeaders(zookeeper, topic._1, simpleConsumers)
      partitionsAndLeaders.foreach(partitionsAndLeader => {
        val topicAndPartition: TopicAndPartition = TopicAndPartition(topic._1, partitionsAndLeader._1.toInt)
        val offsetResponse: OffsetResponse = partitionsAndLeader._2.getOffsetsBefore(OffsetRequest(immutable.Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1))))

        val logSize = offsetResponse.partitionErrorAndOffsets(topicAndPartition).offsets.headOption
        logSize match {
          case Some(size) =>
            result ++= "kafka_logSize{topic=\"%s\",partition=\"%s\"} %d\n".format(topic._1, partitionsAndLeader._1, size)
            topic._2.foreach(topicConsumer => {
              val consumerId: String = topicConsumer._2
              val offset: Long = getConsumerOffset(zookeeper, consumerId, topicConsumer._1, partitionsAndLeader._1)
              result ++= "kafka_offset{topic=\"%s\",consumer=\"%s\",partition=\"%s\"} %d\n".format(topicConsumer._1, consumerId, partitionsAndLeader._1, offset)
              result ++= "kafka_lag{topic=\"%s\",consumer=\"%s\",partition=\"%s\"} %d\n".format(topicConsumer._1, consumerId, partitionsAndLeader._1, size - offset)
            })
          case _ => println("No logSize found for topic: %s and partition: %s".format(topic._1, partitionsAndLeader._1))
        }
      })
    })
    zookeeper.close()
    closeSimpleConsumers(simpleConsumers)
    result ++= "\n"
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
      zookeeper.getChildren("/consumers/%s/offsets".format(consumer), false).map((_, consumer))
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
      id -> consumers.get(partitionInfo.leader).get
    })
  }

  def createConsumers(zookeeper: ZooKeeper): Map[String, SimpleConsumer] = {
    val children: util.List[String] = zookeeper.getChildren("/brokers/ids", false)
    children.map(id => {
      val brokerInfoJson: String = new String(zookeeper.getData("/brokers/ids/" + id, false, null))

      val brokerInfo = gson.fromJson(brokerInfoJson, classOf[BrokerInfo])
      id -> new SimpleConsumer(brokerInfo.getHost, brokerInfo.getPort, 10000, 100000, "consumerOffsetChecker", brokerInfo.getSecurityProtocol)
    }).toMap
  }
}