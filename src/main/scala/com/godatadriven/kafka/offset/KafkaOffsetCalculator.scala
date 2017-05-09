package com.godatadriven.kafka.offset

import java.util

import com.google.gson.{Gson, GsonBuilder}
import kafka.api.{OffsetRequest, OffsetResponse, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import org.apache.zookeeper.KeeperException.NoNodeException
import org.apache.zookeeper.ZooKeeper
import resource._

import scala.collection.JavaConversions._
import scala.collection.{immutable, mutable}
import com.typesafe.config.ConfigFactory
import org.apache.zookeeper.data.Stat

import scala.collection.mutable.ArrayBuffer

case class PartitionInfo(leader: String)

object KafkaOffsetCalculator {
  val gson: Gson = new GsonBuilder().create()

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
          getTopics(zookeeper).filter(_ != "__consumer_offsets").foreach(topic => {
            val partitionsAndLeaders: mutable.Buffer[(String, SimpleConsumer)] = getPartitionsAndLeaders(zookeeper, topic, simpleConsumers)
            partitionsAndLeaders.foreach(partitionsAndLeader => {
              val topicAndPartition: TopicAndPartition = TopicAndPartition(topic, partitionsAndLeader._1.toInt)
              val offsetResponse: OffsetResponse = partitionsAndLeader._2.getOffsetsBefore(OffsetRequest(immutable.Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1))))

              val logSize = offsetResponse.partitionErrorAndOffsets(topicAndPartition).offsets.headOption
              logSize match {
                case Some(size) =>
                  KafkaOffsetConsumer.logSize.put(LogSizeIdentifier(topic, partitionsAndLeader._1), CountOnTime(-1, size))
                  topicsConsumers.getOrDefault(topic, ArrayBuffer.empty).foreach(topicConsumer => {
                    val consumerId: String = topicConsumer._2
                    val offsetIdentifier = OffsetIdentifier(consumerId, topic, partitionsAndLeader._1)
                    val offsetOnTime = getConsumerOffset(zookeeper, offsetIdentifier).getOrElse(CountOnTime(-1, 0))
                    KafkaOffsetConsumer.consumerOffsets.put(offsetIdentifier, offsetOnTime)
                  })
                case _ => println("No logSize found for topic: %s and partition: %s".format(topic, partitionsAndLeader._1))
              }
            })
          })
      }
    }

    KafkaOffsetConsumer.consumerOffsets.foreach { item =>
      val offsetIdentifier = item._1
      val offsetOnTime = item._2
      val logSizeOnTime = KafkaOffsetConsumer.logSize.get(LogSizeIdentifier(item._1.topic, item._1.partition))
      val c: Long = logSizeOnTime.map(_.offset).getOrElse(0)
      result ++= "kafka_offset{topic=\"%s\",consumer=\"%s\",partition=\"%s\"} %d\n".format(offsetIdentifier.topic, offsetIdentifier.group, offsetIdentifier.partition, offsetOnTime.offset)
      result ++= "kafka_lag{topic=\"%s\",consumer=\"%s\",partition=\"%s\"} %d\n".format(offsetIdentifier.topic, offsetIdentifier.group, offsetIdentifier.partition, c - offsetOnTime.offset)
    }
    KafkaOffsetConsumer.logSize.foreach{ item =>
      result ++= "kafka_logSize{topic=\"%s\",partition=\"%s\"} %d\n".format(item._1.topic, item._1.partition, item._2.offset)
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

  def getConsumerOffset(zookeeper: ZooKeeper, offsetIdentifier: OffsetIdentifier): Option[CountOnTime] = {
    try {
      val path = "/consumers/%s/offsets/%s/%s".format(offsetIdentifier.group, offsetIdentifier.topic, offsetIdentifier.partition)
      val stat = new Stat()
      val offset = new String(zookeeper.getData(path, false, stat)).toLong

      Some(CountOnTime(stat.getMtime, offset))
    } catch {
      case e: NoNodeException =>
        None
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

