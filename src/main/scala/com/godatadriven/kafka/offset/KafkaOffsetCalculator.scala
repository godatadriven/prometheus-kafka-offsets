package com.godatadriven.kafka.offset

import java.util

import com.google.gson.GsonBuilder
import kafka.api.{OffsetRequest, OffsetResponse, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import org.apache.zookeeper.KeeperException.NoNodeException
import org.apache.zookeeper.ZooKeeper
import resource._

import scala.collection.JavaConversions._
import scala.collection.{immutable, mutable}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable.ArrayBuffer

case class PartitionInfo(leader: String)

object KafkaOffsetCalculator {
  val gson = new GsonBuilder().create()

  def main(args: Array[String]) {
    println(getTopicOffset)
  }

  def getTopicOffset: String = {
    val result = new StringBuilder
    val config = ConfigFactory.load
    val zookeeperUrl = config.getString("zookeeper.url")

    managed(new ZooKeeper(zookeeperUrl, 10000, null, true)) acquireAndGet {
      zookeeper => managed(new SimpleConsumers(zookeeper)) acquireAndGet {
        simpleConsumers =>
          val topicsConsumers = getTopicConsumers(zookeeper)
          getTopics(zookeeper).foreach(topic => {
            val partitionsAndLeaders: mutable.Buffer[(String, SimpleConsumer)] = getPartitionsAndLeaders(zookeeper, topic, simpleConsumers)
            partitionsAndLeaders.foreach(partitionsAndLeader => {
              val topicAndPartition: TopicAndPartition = TopicAndPartition(topic, partitionsAndLeader._1.toInt)
              val offsetResponse: OffsetResponse = partitionsAndLeader._2.getOffsetsBefore(OffsetRequest(immutable.Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1))))

              val logSize = offsetResponse.partitionErrorAndOffsets(topicAndPartition).offsets.headOption
              logSize match {
                case Some(size) =>
                  result ++= "kafka_logSize{topic=\"%s\",partition=\"%s\"} %d\n".format(topic, partitionsAndLeader._1, size)
                  topicsConsumers.getOrDefault(topic, ArrayBuffer.empty).foreach(topicConsumer => {
                    val consumerId: String = topicConsumer._2
                    val offset: Long = getConsumerOffset(zookeeper, consumerId, topic, partitionsAndLeader._1)
                    result ++= "kafka_offset{topic=\"%s\",consumer=\"%s\",partition=\"%s\"} %d\n".format(topic, consumerId, partitionsAndLeader._1, offset)
                    result ++= "kafka_lag{topic=\"%s\",consumer=\"%s\",partition=\"%s\"} %d\n".format(topic, consumerId, partitionsAndLeader._1, size - offset)
                  })
                case _ => println("No logSize found for topic: %s and partition: %s".format(topic, partitionsAndLeader._1))
              }
            })
          })
      }
    }

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

  def getTopics(zookeeper: ZooKeeper): util.List[String] = {
        zookeeper.getChildren("/brokers/topics", false)
    }

  def getConsumerOffset(zookeeper: ZooKeeper, group_id: String, topic: String, partition: String): Long = {
    try {
      new String(zookeeper.getData("/consumers/%s/offsets/%s/%s".format(group_id, topic, partition), false, null)).toLong
    } catch {
      case e: NoNodeException =>
        0L
    }
  }

  def getPartitionsAndLeaders(zookeeper: ZooKeeper, topic: String, consumers: SimpleConsumers): mutable.Buffer[(String, SimpleConsumer)] = {
    val children: util.List[String] = zookeeper.getChildren("/brokers/topics/%s/partitions".format(topic), false)
    children.flatMap(id => {
      val partitionJson: String = new String(zookeeper.getData("/brokers/topics/%s/partitions/%s/state".format(topic, id), false, null))
      val partitionInfo = gson.fromJson(partitionJson, classOf[PartitionInfo])
      val consumer: SimpleConsumer = consumers.get(partitionInfo.leader).orNull
      if (consumer != null) {
        Some(id -> consumer)
      } else {
        None
      }
    })
  }

}