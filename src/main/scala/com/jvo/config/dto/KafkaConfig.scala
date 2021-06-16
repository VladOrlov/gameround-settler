package com.jvo.config.dto

case class KafkaConfig(bootstrapServers: String,
                       offsetReset: String,
                       enableAutoCommit: Boolean,
                       consumerGroup: String,
                       topics: KafkaTopics)