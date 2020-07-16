package core

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}



object ApplicationContext {
  val warehouseLocation = new Path("/apps/hive/warehouse").toString

  def sparkSession : SparkSession = SparkSession.builder()
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()

  private def hadoopConfiguration = sparkSession.sparkContext.hadoopConfiguration

  def defaultFS : FileSystem = FileSystem.get(hadoopConfiguration)
}
