package com.jvo.config

import com.jvo.config.dto.{ApplicationConfiguration, CassandraConfig, KafkaConfig, SparkConfig}
import wvlet.airframe.config.Config

trait ConfigurableStreamingApp {

  def loadConfiguration(): ApplicationConfiguration = {
    val config: Config = Config(env = "development", configPaths = Seq("./config"))
      .registerFromYaml[KafkaConfig]("kafka-config.yml")
      .registerFromYaml[SparkConfig]("spark-config.yml")
      .registerFromYaml[CassandraConfig]("cassandra-config.yml")

    ApplicationConfiguration(
      kafkaConfig = config.of[KafkaConfig],
      sparkConfig = config.of[SparkConfig],
      cassandraConfig = config.of[CassandraConfig]
    )
  }
}