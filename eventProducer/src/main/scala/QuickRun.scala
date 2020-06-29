import net.liftweb.json.{DefaultFormats, parse}

import scala.io.Source

object QuickRun extends App {

  implicit val formats = DefaultFormats

  val metadata = Source.fromFile("/home/sekiro/projects/dataEngine/eventProducer/src/test/resources/user_metadata.json")
  val clientMetadata = parse(metadata.mkString)
  val parsedClientMetadata = clientMetadata.extract[MetaData]

  val notificationMessage = StringBuilder.newBuilder

  notificationMessage.append(
    s"""{
       |"timestamp" : "${System.currentTimeMillis()}",
       |"targetDB" : "${parsedClientMetadata.targetDB}",
       |"targetTableName" : "${parsedClientMetadata.targetTableName}",
       |"folderSrc" : "${parsedClientMetadata.folderSrc}",
       |"targetPath" : "${parsedClientMetadata.targetPath}",
       |"partitionColumn" : "${parsedClientMetadata.partitionColumn}",
       |}""".stripMargin)

  println(notificationMessage.toString())

}
