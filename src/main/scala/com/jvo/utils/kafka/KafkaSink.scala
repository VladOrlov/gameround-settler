package com.jvo.utils.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.collection.JavaConversions._

object KafkaSink {

  /**
   * A constructor for the KafkaSink class.
   *
   * @param bootstrapServers A comma-separated string of bootstrap servers also known as Kafka Brokers.
   * @param retries Number of retries in case of failure in the event's sending. Default 5.
   * @return A KafkaSink object.
   */
  def apply(bootstrapServers: String, retries: Int = DEFAULT_RETRIES): KafkaSink = {
    // Build the function to be passed to the class
    val initKafkaProducer = () => {
      val config = getProducerConfig(bootstrapServers, retries)

      val producer = new KafkaProducer[String, String](config)
      sys.addShutdownHook { producer.close() }
      producer
    }

    // Return an instance of the wrapped Kafka Producer
    new KafkaSink(initKafkaProducer)
  }

  // Public Constants
  final val AUTO_OFFSET_RESET_LATEST = "latest"
  final val AUTO_OFFSET_RESET_EARLIEST = "earliest"
  final val AUTO_COMMIT_TRUE: java.lang.Boolean = true
  final val AUTO_COMMIT_FALSE: java.lang.Boolean = false
  final val DEFAULT_RETRIES: java.lang.Integer = 5

  /**
   * Build the Map containing all the necessary options for a Kafka Consumer.
   * It also provides String Deserializer for both Key and Value of a message and the use of Kafka itself for storing offsets.
   *
   * @param brokers The comma-separated list of brokers (with port)
   * @param groupId The Group ID to use to stream from a Kafka topic. Usually the app's name.
   * @param autoReset If to start from the beginning (AUTO_OFFSET_RESET_EARLIEST) or from the end (AUTO_OFFSET_RESET_LATEST - default) of the queue.
   * @param autoCommit True to commit in Kafka every read message, false otherwise. Default true.
   * @return A Map object with all the parameters necessary to build an instance of a Kafka Consumer
   */
  def getConsumerConfig(brokers: String,
                        groupId: String,
                        autoReset: String = AUTO_OFFSET_RESET_LATEST,
                        autoCommit: java.lang.Boolean = AUTO_COMMIT_TRUE): Map[String, Object] = {
    Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "offsets.storage" -> "kafka", // better then ZooKeeper in terms of throughput
      "auto.offset.reset" -> autoReset,
      "enable.auto.commit" -> autoCommit
    )
  }

  /**
   * Build the Map containing all the necessary options for a Kafka Producer.
   * It also provides String Deserializer for both Key and Value of a message.
   *
   * @param brokers The comma-separated list of brokers (with port)
   * @param retries The number of times the producer will try to send the message in case of failures. Default 5.
   * @return A Map object with all the parameters necessary to build an instance of a Kafka Consumer
   */
  def getProducerConfig(brokers: String, retries: java.lang.Integer = 5): Map[String, Object] = {
    Map[String, Object](
      "bootstrap.servers" -> brokers,
      "retries"           -> retries,
      "key.serializer"    -> classOf[StringSerializer],
      "value.serializer"  -> classOf[StringSerializer]
    )
  }

}

/**
 * A serializable class which defines a Kafka Producer and the correspondent functions to send messages to Kafka.
 *
 * @param createProducer A function that returns an instance of KafkaProducer with key and value as String.
 */
final class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {

  /**
   * A static lazy holder of the Kafka Producer.
   */
  private[this] lazy val producer = createProducer()

  /**
   * A function that sends an event to Kafka.
   *
   * @param topic The name of the Kafka topic.
   * @param key The string key to be used for the event.
   * @param value The string value to be used for the event.
   * @param headers A list of kafka headers to be sent along with the event.
   */
  def send(topic: String, key: String, value: String, headers: List[Header]): Unit = {
    producer.send(new ProducerRecord(topic, null, key, value, headers))
  }

  /**
   * A function that sends an event to Kafka.
   *
   * @param topic The name of the Kafka topic.
   * @param key The string key to be used for the event.
   * @param value The string value to be used for the event.
   */
  def send(topic: String, key: String, value: String): Unit = {
    producer.send(new ProducerRecord(topic, key, value))
  }
}