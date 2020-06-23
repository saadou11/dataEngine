package core

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

object ApplicationContext {


  def sparkSession : SparkSession = SparkSession.builder()
    .enableHiveSupport()
    .getOrCreate()

  private def hadoopConfiguration = sparkSession.sparkContext.hadoopConfiguration

  def defaultFS : FileSystem = FileSystem.get(hadoopConfiguration)
}
