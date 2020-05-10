package utils

import java.lang.management.ManagementFactory

import org.apache.spark.sql.SparkSession

object SparkUtils {

  val isIDE = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
  }

  def getSparkSession(appName: String) = {

    var checkpointDir = ""

    val sessionBuilder = SparkSession
      .builder()

    // Check if running from IDE
    if (isIDE) {
      //System.setProperty("hadoop.home.dir", "D:\\hadoop-2.10.0\\bin") // required for winutils
      sessionBuilder.master("local")
      checkpointDir = "file:///d:/temp"
    }else{
      checkpointDir = "hdfs://lambda-pluralsight:9000/spark/checkpoint"
    }

    val spark = sessionBuilder
      .appName(appName)
      .getOrCreate()

    spark

  }
}
