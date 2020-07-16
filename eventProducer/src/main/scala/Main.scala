

import core.{ApplicationContext, ApplicationProperties}
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import net.liftweb.json._
import net.liftweb.json.DefaultFormats
import org.slf4j.{Logger, LoggerFactory}
import utils.KafkaUtils

import scala.io.Source

case class MetaData(targetDB: String, targetTableName: String, folderSrc: String, targetPath: String, partitionColumn: String,schema: String)

object Main extends App {

  def log : Logger = LoggerFactory.getLogger( Main.getClass )

  implicit val formats = DefaultFormats


  val sparkSession = ApplicationContext.sparkSession

  val topic = sparkSession.conf.get(ApplicationProperties.KAFKA_TOPIC)
  val kafkaProducerProperties = KafkaUtils.initKafkaProducerProperties()

  val producer = new KafkaProducer[String, String](kafkaProducerProperties)
  log.info("create kafka producer")

  val sc = sparkSession.sparkContext
  val conf = sc.hadoopConfiguration
  val fs = FileSystem.get(conf)
  val rootFolder = fs.listStatus(new Path("/landing-area"))


  val notificationMessage = StringBuilder.newBuilder

  rootFolder.foreach(x => {

    val dataSetName = x.getPath().getName
    val files = x.getPath.getFileSystem(conf).listStatus(new Path(s"/landing-area/$dataSetName"))
    log.info("get list files path")

    files.foreach(file => {

      if (file.getPath().getName.endsWith(".json")) {
        val buildMetaData = StringBuilder.newBuilder
        val metadata = sc.textFile(file.getPath().toString)

        metadata.collect().foreach(line => buildMetaData.append(line))

        val clientMetadata = parse(buildMetaData.toString)
        val parsedClientMetadata = clientMetadata.extract[MetaData]
        log.info("creating metadata object")

        notificationMessage.append(
          s"""{
             |"timestamp" : "${System.currentTimeMillis()}",
             |"targetDB" : "${parsedClientMetadata.targetDB}",
             |"targetTableName" : "${parsedClientMetadata.targetTableName}",
             |"folderSrc" : "${parsedClientMetadata.folderSrc}",
             |"targetPath" : "${parsedClientMetadata.targetPath}",
             |"partitionColumn" : "${parsedClientMetadata.partitionColumn}",
             |"schema" : "${parsedClientMetadata.schema}",
             |}""".stripMargin
        )
        log.info("creating kafka message record")

      }

      else {
        val data_string = file.getPath().getName
        val data = file.getPath.getFileSystem(conf).listStatus(new Path(s"/landing-area/$dataSetName/$data_string"))

        data.foreach(dataFile => {

          val srcPath = new Path(dataFile.getPath.toString)
          val dstPath = new Path(s"/tmp/$dataSetName/$data_string/${dataFile.getPath.getName}")

          FileUtil.copy(srcPath.getFileSystem(conf), srcPath, dstPath.getFileSystem(conf), dstPath, true, conf)
          log.info("sending data to tmp folder...")
        })
      }

    })
  })

  /**
   * @TODO delete all files in landing-area
   *       // FileUtil.fullyDeleteContents(new File(r.toString),)
   */


  val record = new ProducerRecord[String, String](topic, notificationMessage.toString())

  producer.send(record)
  log.info("sending record in kafka topic")

  KafkaUtils.flushAndClose(producer)

}
