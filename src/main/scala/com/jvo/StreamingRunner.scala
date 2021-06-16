package com.jvo

import com.jvo.config.dto.{ApplicationConfiguration, KafkaConfig, SparkConfig}
import com.jvo.config.{ConfigurableStreamingApp, KafkaComponent, SparkComponent}
import com.jvo.dto._
import com.jvo.service.{DatabaseService, GameRoundSettler}
import com.jvo.utils.JsonObjectMapper
import com.jvo.utils.kafka.KafkaSink
import org.apache.log4j.LogManager
import org.apache.spark.broadcast.Broadcast


object StreamingRunner extends ConfigurableStreamingApp with KafkaComponent with SparkComponent {

  private[this] final val log = LogManager.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val applicationConfiguration = loadConfiguration()
    val ApplicationConfiguration(kafkaConfig, sparkConfig, cassandraConfig) = applicationConfiguration

    val ssc = initSparkStreamingContext(applicationConfiguration)
    val kafkaInputStream = getKafkaInputStream(ssc, applicationConfiguration)

    implicit val kafkaConfigBroadcast: Broadcast[KafkaConfig] = ssc.sparkContext.broadcast(kafkaConfig)
    implicit val sparkConfigBroadcast: Broadcast[SparkConfig] = ssc.sparkContext.broadcast(sparkConfig)
    implicit val kafkaSinkBroadcast: Broadcast[KafkaSink] = ssc.sparkContext.broadcast(KafkaSink(kafkaConfig.bootstrapServers))
    val databaseService = DatabaseService(cassandraConfig, ssc.sparkContext.getConf)
    val gameRoundSettler = GameRoundSettler(databaseService)
    implicit val gameRoundSettlerBroadcast: Broadcast[GameRoundSettler] = ssc.sparkContext.broadcast(gameRoundSettler)

    kafkaInputStream
      .map(message => (message.key(), JsonObjectMapper.parseToWalletTransaction(message.value())))
      .groupByKey(sparkConfig.partitionsNumber)
      .map(groupedTransactions => CustomerWalletTransactions(groupedTransactions._1, groupedTransactions._2.toSeq))
      .flatMap(transaction => gameRoundSettlerBroadcast.value.processGameRoundFinalTransaction(transaction))
      .foreachRDD(rdd => rdd.foreach { lossEvent =>
        sendMessageToKafkaTopic(lossEvent)
      })

    ssc.start()
    ssc.awaitTermination()
  }

  private def sendMessageToKafkaTopic(lossCheckEvent: LossCheckEvent)
                                     (implicit kafkaConfigBroadcast: Broadcast[KafkaConfig],
                                      kafkaSinkBroadcast: Broadcast[KafkaSink]): Unit = {

    publishToKafkaTopic(kafkaSinkBroadcast)(
      getDestinationTopic,
      lossCheckEvent.individualTaxNumber,
      JsonObjectMapper.toJson(lossCheckEvent))
  }

  private def publishToKafkaTopic(kafkaSinkBroadcast: Broadcast[KafkaSink]) = {
    kafkaSinkBroadcast.value.send(_: String, _: String, _: String)
  }

  private def getDestinationTopic(implicit kafkaConfigBroadcast: Broadcast[KafkaConfig]) = {
    kafkaConfigBroadcast.value.topics.destination
  }


}
