import java.util

import core.{ApplicationContext, ApplicationProperties}
import net.liftweb.json.{DefaultFormats, parse}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.{Logger, LoggerFactory}
import utils.KafkaUtils

import scala.collection.JavaConverters._

object MainIngestion extends App {
  def log : Logger = LoggerFactory.getLogger( MainIngestion.getClass )

  val sparkSession = ApplicationContext.sparkSession

  implicit val formats = DefaultFormats


  case class MetaData(targetDB: String, targetTableName: String, folderSrc: String,targetPath : String, partitionColumn : String,schema: String)

  def buildMetaData(record : String) : MetaData = {
    parse(record).extract[MetaData]
  }

  val pros = KafkaUtils.initKafkaConsumerProperties()

  val topic = sparkSession.conf.get(ApplicationProperties.KAFKA_TOPIC)

  val consumer : KafkaConsumer[String,String] = new KafkaConsumer(pros)
  log.info("create kafka consumer")

  consumer.subscribe(util.Collections.singletonList(topic))
  log.info("subscribe to kafka topic")

  var event : MetaData = null
  var loopCondintion = true

  while(loopCondintion) {

    val records = consumer.poll(100)

    for (record <- records.asScala) {

      event = buildMetaData(record.value())
      log.info("get record from topic")

      val sqlContext = sparkSession.sqlContext

      import sparkSession.implicits._
      import sparkSession.sql
      import sqlContext.implicits._

      sqlContext.sql("CREATE DATABASE IF NOT EXISTS " + event.targetDB).show()
      log.info("creating database...")

      sparkSession.catalog.setCurrentDatabase(event.targetDB)

      /**
       * TODO drop table in exist
       */
      sqlContext.sql("CREATE TABLE IF NOT EXISTS " + event.targetTableName + event.schema + " ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n'").show
      log.info("creating table...")

      sqlContext.sql("LOAD DATA INPATH " + "'" +event.folderSrc + "data/" +  "'" +" INTO TABLE " + event.targetTableName).show()
      log.info("load data into table")

      consumer.commitSync()

      loopCondintion = false
    }
  }

}
