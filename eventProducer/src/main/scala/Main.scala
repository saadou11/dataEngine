import java.io.File

import core.{ApplicationContext, ApplicationProperties}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import net.liftweb.json._
import net.liftweb.json.DefaultFormats
import utils.KafkaUtils

import scala.io.{BufferedSource, Source}



object Main {

  implicit val formats = DefaultFormats

  case class MetaData(targetDB: String, targetTableName: String, folderSrc: String,targetPath : String, partitionColumn : String)


  def main(args: Array[String]): Unit = {

    val sparkSession = ApplicationContext.sparkSession

    val sc = sparkSession.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val status = fs.listStatus(new Path("/landing-area"))

    val parsedClientMetadata : MetaData = null


    status.foreach(x => {
      val nomClient = x.getPath().getName
      val files = x.getPath.getFileSystem(conf).listStatus(new Path(s"/landing-area/$nomClient"))

      files.foreach(file => {

        if (file.getPath().getName.endsWith(".json")){

          val metadata = Source.fromFile(file.getPath().toString)
          val clientMetadata = parse(metadata.mkString)
          val parsedClientMetadata  = clientMetadata.extract[MetaData]

        }

        else
          {
            val data_string = file.getPath().getName
            val data = file.getPath.getFileSystem(conf).listStatus(new Path(s"/landing-area/$nomClient/$data_string"))
            data.foreach(dataFile => {
              val srcPath = new Path(dataFile.getPath.toString)
              val dstPath = new Path(s"/tmp/$nomClient/$data_string/${dataFile.getPath.getName}")
              FileUtil.copy(srcPath.getFileSystem(conf), srcPath, dstPath.getFileSystem(conf), dstPath, true, conf)
            } )
          }

      })
    })

    /**
     * @TODO delete all files in landing-area
     *      // FileUtil.fullyDeleteContents(new File(r.toString),)
     */

    val topic = sparkSession.conf.get(ApplicationProperties.KAFKA_TOPIC)

    val kafkaProducerProperties = KafkaUtils.initKafkaProducerProperties()

    val producer = new KafkaProducer[String, String](kafkaProducerProperties)

    val message =
      s"""{
         |"timestamp" : "${System.currentTimeMillis()}",
         |"targetDB" : "${parsedClientMetadata.targetDB}",
         |"targetTableName" : "${parsedClientMetadata.targetTableName}",
         |"folderSrc" : "${parsedClientMetadata.folderSrc}",
         |"targetPath" : "${parsedClientMetadata.targetPath}",
         |"partitionColumn" : "${parsedClientMetadata.partitionColumn}",
         |}""".stripMargin


    val record = new ProducerRecord[String,String](topic,message)

    producer.send(record)

    KafkaUtils.flushAndClose(producer)

  }
}
