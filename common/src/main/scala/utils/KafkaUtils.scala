package utils

import java.util.Properties

import core.{ApplicationContext, ApplicationProperties}
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.SparkSession


object KafkaUtils {

  val spark: SparkSession = ApplicationContext.sparkSession

  /**
   *
   * When using Schema registry
   *
   * @return Kafka Producer Properties
   *
   */

  def initKafkaProducerProps(): Properties = {

    /*Initializing a java.util.properties to be used as map parameters for the producer*/

    val producerConfig = new Properties()


    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, spark.conf.get(ApplicationProperties.KAFKA_BOOTSTRAP_SERVER))
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all")
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])



    producerConfig

  }

  /**
   *
   *
   *
   * @return Kafka Producer Properties without schema registry
   *
   */

  def initKafkaProducerProperties() = {

    /*Initializing a java.util.properties to be used as map parameters for the producer*/

    val producerConfig = new Properties()


    producerConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, spark.conf.get(ApplicationProperties.KAFKA_BOOTSTRAP_SERVER))
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all")
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    producerConfig

  }

  /**
   *
   *
   *
   * @return Kafka Consumer Properties
   *
   */

  def initKafkaConsumerProperties(): Properties = {

    /*Initializing a java.util.properties to be used as map parameters for the consumer*/

    val consumerConfig = new Properties()


    consumerConfig.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")


   consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, spark.conf.get(ApplicationProperties.KAFKA_BOOTSTRAP_SERVER))
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, spark.conf.get(ApplicationProperties.KAFKA_AUTO_OFFSET_RESET))
    consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, spark.conf.get(ApplicationProperties.KAFKA_ENABLE_AUTO_COMMIT))
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, spark.conf.get(ApplicationProperties.KAFKA_CONSUMER_GROUP))
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    consumerConfig

  }

  /**
   *
   *
   *
   * @param properties
   * @param keyClass
   * @param valueClass
   * @return switches the serializing class for producing purpose
   *
   */

  def switchSerializer(properties: Properties, keyClass: Class[_], valueClass: Class[_]): Properties = {
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keyClass)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueClass)
    properties

  }


  /**
   *
   *
   *
   * @param properties
   * @param keyClass
   * @param valueClass
   * @return switches the deserializer class for consuming purpose
   *
   */

  def switchDeserializer(properties: Properties, keyClass: Class[_], valueClass: Class[_]): Properties = {
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyClass)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueClass)
    properties

  }


  /**
   *
   * @param producer
   * @tparam A
   * @todo work in progress
   *
   */

  def flushAndClose[A](producer: KafkaProducer[A, A]) = {

    producer.flush()
    producer.close()

  }

}