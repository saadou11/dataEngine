import java.util

import core.{ApplicationContext, ApplicationProperties}
import net.liftweb.json.{DefaultFormats, parse}
import org.apache.kafka.clients.consumer.KafkaConsumer
import utils.KafkaUtils

import scala.collection.JavaConverters._

object MainIngestion extends App {

  val sparkSession = ApplicationContext.sparkSession

  implicit val formats = DefaultFormats


  case class MetaData(targetDB: String, targetTableName: String, folderSrc: String,targetPath : String, partitionColumn : String)

  def buildMetaData(record : String) : MetaData = {
    parse(record).extract[MetaData]
  }

  val pros = KafkaUtils.initKafkaConsumerProperties()

  val topic = sparkSession.conf.get(ApplicationProperties.KAFKA_TOPIC)

  val consumer : KafkaConsumer[String,String] = new KafkaConsumer(pros)

  consumer.subscribe(util.Collections.singletonList(topic))

  var event : MetaData = null
  var loopCondintion = true
  while(loopCondintion) {
    val records = consumer.poll(100)
    for (record <- records.asScala) {
      event = buildMetaData(record.value())
      println("/////////in consumer loop : " + record.value())


      println("/////////in while loop : " + event.targetDB)


      println("//////////////////////////////////////")
      println("target : " + event.targetDB)
      val sqlContext = sparkSession.sqlContext

      import sparkSession.implicits._
      import sparkSession.sql
      import sqlContext.implicits._

      sqlContext.sql("CREATE DATABASE IF NOT EXISTS " + event.targetDB).show()

      sparkSession.catalog.setCurrentDatabase(event.targetDB)

      /**
       * TODO drop table in exist
       */
      sqlContext.sql("CREATE TABLE IF NOT EXISTS " + event.targetTableName + sparkSession.conf.get(ApplicationProperties.SCHEMA) + " ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n'").show

      sqlContext.sql("LOAD DATA INPATH " + "'" +event.folderSrc + "data/" +  "'" +" INTO TABLE " + event.targetTableName).show()
      consumer.commitSync()
      loopCondintion = false
    }
  }

}
