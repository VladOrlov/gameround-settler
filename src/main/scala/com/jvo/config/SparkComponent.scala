package com.jvo.config

import com.jvo.config.dto.{ApplicationConfiguration, CassandraConfig, KafkaConfig, KafkaTopics, SparkConfig}
import com.jvo.dto.{BigLossEvent, CustomerPlayResult, CustomerWalletTransactions, GameRoundResult, WalletTransaction}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait SparkComponent {

  def initSparkStreamingContext(applicationConfiguration: ApplicationConfiguration): StreamingContext = {

    val ApplicationConfiguration(_, sparkConfig: SparkConfig, cassandraConfig: CassandraConfig) = applicationConfiguration

    val spark = SparkSession.builder()
      .config(new SparkConf(true)
        .setAppName(sparkConfig.applicationName)
        .set("spark.cassandra.connection.host", cassandraConfig.host)
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .registerKryoClasses(getKryoClasses)
        .setMaster("local[2]")
      ).getOrCreate()

    new StreamingContext(spark.sparkContext, Seconds(sparkConfig.batchDuration))
  }

  private def getKryoClasses = {
    Array[Class[_]](
      classOf[ApplicationConfiguration],
      classOf[CassandraConfig],
      classOf[KafkaConfig],
      classOf[KafkaTopics],
      classOf[SparkConfig],
      classOf[BigLossEvent],
      classOf[CustomerPlayResult],
      classOf[CustomerWalletTransactions],
      classOf[GameRoundResult],
      classOf[WalletTransaction],
      classOf[CassandraConfig],
    )
  }
}
