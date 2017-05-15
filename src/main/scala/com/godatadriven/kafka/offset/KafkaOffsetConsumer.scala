package com.godatadriven.kafka.offset

import java.nio.ByteBuffer

import kafka.message.MessageAndMetadata
import org.apache.kafka.common.protocol.types.{Field, Schema, Struct}
import org.apache.kafka.common.protocol.types.Type.STRING
import org.apache.kafka.common.protocol.types.Type.INT32
import org.apache.kafka.common.protocol.types.Type.INT64

import scala.collection.mutable

object KafkaOffsetConsumer {

  val consumerOffsets: mutable.Map[OffsetIdentifier, CountOnTime] = mutable.Map[OffsetIdentifier, CountOnTime]()
  val logSize: mutable.Map[LogSizeIdentifier, CountOnTime] = mutable.Map[LogSizeIdentifier, CountOnTime]()

  case class MessageValueStructAndVersion(value: Struct, version: Short)

  case class KeyAndValueSchemas(keySchema: Schema, valueSchema: Schema)

  private val OFFSET_COMMIT_KEY_SCHEMA = new Schema(new Field("group", STRING),
    new Field("topic", STRING),
    new Field("partition", INT32))
  private val KEY_GROUP_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("group")
  private val KEY_TOPIC_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("topic")
  private val KEY_PARTITION_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("partition")

  private val OFFSET_COMMIT_VALUE_SCHEMA_V1 = new Schema(new Field("offset", INT64),
    new Field("metadata", STRING, "Associated metadata.", ""),
    new Field("commit_timestamp", INT64),
    new Field("expire_timestamp", INT64))

  def run(): Unit = {
    val props = new java.util.Properties()
    props.put("zookeeper.connect", "localhost:2181")
    props.put("group.id", "offsetTester5")
    props.put("zookeeper.session.timeout.ms", "5000")
    //    props.put("rebalance.backoff.ms", "5000");
    props.put("zookeeper.sync.time.ms", "2000")
    props.put("auto.commit.interval.ms", "5000")
    //    props.put("autooffset.reset", "smallest");
    props.put("exclude.internal.topics", "false")
    val config = new kafka.consumer.ConsumerConfig(props)
    val consumer = kafka.consumer.Consumer.create(config)

    val topic = "__consumer_offsets"
    val numThread = 1
    val topicCounts = Map(topic -> numThread)
    val consumerMap = consumer.createMessageStreams(topicCounts)

    val consumerIterator = consumerMap(topic).head.iterator()

    consumerIterator.foreach { msg: MessageAndMetadata[Array[Byte], Array[Byte]] =>
      try {
        val offsetIdentifier = readMessageKey(ByteBuffer.wrap(msg.key))
        val offsetOnTime = readMessageValue(ByteBuffer.wrap(msg.message), offsetIdentifier)
        consumerOffsets.put(offsetIdentifier, offsetOnTime)
      } catch {
        case e: Exception => print(e.getMessage +  "message: " + msg.message().toString)
      }
    }
  }


  private def readMessageKey(buffer: ByteBuffer): OffsetIdentifier = {
    val version = buffer.getShort()
    val key = OFFSET_COMMIT_KEY_SCHEMA.read(buffer)

    val group = key.get(KEY_GROUP_FIELD).asInstanceOf[String]
    val topic = key.get(KEY_TOPIC_FIELD).asInstanceOf[String]
    val partition = key.get(KEY_PARTITION_FIELD).asInstanceOf[Int]

    OffsetIdentifier(group, topic, partition.toString)
  }

  private def readMessageValue(buffer: ByteBuffer, consumerOffset: OffsetIdentifier): CountOnTime = {
    val structAndVersion = readMessageValueStruct(buffer)

    if (structAndVersion.value == null) {
      // tombstone
      null
    } else {
      if (structAndVersion.version == 1) {
        val offset = structAndVersion.value.get(OFFSET_COMMIT_VALUE_SCHEMA_V1.get("offset")).asInstanceOf[Long]
        val commitTimestamp = structAndVersion.value.get(OFFSET_COMMIT_VALUE_SCHEMA_V1.get("commit_timestamp")).asInstanceOf[Long]
        val metadata = structAndVersion.value.get(OFFSET_COMMIT_VALUE_SCHEMA_V1.get("metadata")).asInstanceOf[String]
        val expireTimestamp = structAndVersion.value.get(OFFSET_COMMIT_VALUE_SCHEMA_V1.get("expire_timestamp")).asInstanceOf[Long]

        CountOnTime(commitTimestamp, offset)
      } else {
        throw new IllegalStateException("Unknown offset message version: " + structAndVersion.version)
      }
    }
  }

  private def readMessageValueStruct(buffer: ByteBuffer): MessageValueStructAndVersion = {
    if (buffer == null) {
      // tombstone
      MessageValueStructAndVersion(null, -1)
    } else {
      val version = buffer.getShort()
      val value = OFFSET_COMMIT_VALUE_SCHEMA_V1.read(buffer)

      MessageValueStructAndVersion(value, version)
    }
  }
}
