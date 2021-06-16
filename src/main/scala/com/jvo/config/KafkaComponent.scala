package com.jvo.config

import com.jvo.config.dto.{ApplicationConfiguration, KafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

trait KafkaComponent {

  def getKafkaInputStream(ssc: StreamingContext,
                          applicationConfiguration: ApplicationConfiguration): InputDStream[ConsumerRecord[String, String]] = {

    val kafkaTopics = applicationConfiguration.kafkaConfig.topics

    val topics = List(kafkaTopics.source)
    val kafkaDStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, getKafkaParams(applicationConfiguration.kafkaConfig))
    )

    kafkaDStream
  }

  private def getKafkaParams(kafkaConfig: KafkaConfig): Map[String, Object] = {

    val kafkaParams: Map[String, Object] = Map(
      "bootstrap.servers" -> kafkaConfig.bootstrapServers,
      "key.serializer" -> classOf[StringSerializer],
      "value.serializer" -> classOf[StringSerializer],
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> kafkaConfig.offsetReset,
      "enable.auto.commit" -> kafkaConfig.enableAutoCommit.asInstanceOf[Object],
      "group.id" -> kafkaConfig.consumerGroup
    )

    kafkaParams
  }

}