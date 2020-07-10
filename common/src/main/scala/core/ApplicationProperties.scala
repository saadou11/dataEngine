package core

object ApplicationProperties {

  /**
   * Application
   */

  val APPLICATION_NAMESPACE = ""
  val SEMICOLON = ";"


  /**
   * Ingestion directories
   */

  val META_FILE = "spark.meta.file"
  val WORKING_AREA = "/lake/bsc/a8579_igd/file_producer"
  val SRV_AREA = "/srv"


  /**
   * output format
   */

  val DATA_FORMAT = "spark.data.format"
  val SCHEMA = "spark.output.schema"




  /**
   * List of Kafka properties
   */

  val KAFKA_BOOTSTRAP_SERVER = "spark.kafka.bootstrap.server"
  val KAFKA_AUTO_OFFSET_RESET = "spark.kafka.auto.offset.reset"
  val KAFKA_ENABLE_AUTO_COMMIT = "spark.kafka.enable.auto.commit"
  val KAFKA_TOPIC = "spark.kafka.topic"
  val KAFKA_ERRORS_TOPIC = "spark.kafka.errors.topic"
  val KAFKA_CONSUMER_GROUP = "spark.kafka.consumer.group"
  val KAFKA_CURRENT_OFFSET = "spark.kafka.current.offset"



  val KAFKA_NOTIFICATION_TOPIC = "spark.kafka.notification.topic"
  val KAFKA_MONITORING_TOPIC = "spark.kafka.monitoring.topic"





  // SCHEMA REGISTRY

  val SCHEMA_REGISTRY_URL = "spark.kafka.schema.registry.url"





  /**
   * Properties to sleep threads
   */

  val MINUTE = 60000
  val HOUR = MINUTE * 60
  val DAY = HOUR * 24

}
