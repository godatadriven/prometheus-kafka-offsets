package com.godatadriven.kafka.offset

case class OffsetIdentifier(group: String, topic: String, partition: String)
case class LogSizeIdentifier(topic: String, partition: String)
case class CountOnTime(modificationTime: Long, offset: Long)