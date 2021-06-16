package com.jvo.config.dto

case class ApplicationConfiguration(kafkaConfig: KafkaConfig, sparkConfig: SparkConfig, cassandraConfig: CassandraConfig)