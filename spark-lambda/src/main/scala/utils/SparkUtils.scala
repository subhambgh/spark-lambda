package utils

import java.lang.management.ManagementFactory

import org.apache.spark.{SparkConf, SparkContext, streaming}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}

object SparkUtils {

  val isIDE = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
  }

  def getSparkContext(appName: String) = {

    var checkpointDir = ""

    // get spark configuration
    val conf = new SparkConf()
      .setAppName("Lambda with Spark")

    // Check if running from IDE
    if (isIDE) {
      System.setProperty("hadoop.home.dir", "D:\\hadoop-2.10.0\\bin") // required for winutils
      conf.setMaster("local[*]")
      checkpointDir = "file:///d:/temp"
    }else{
      checkpointDir = "hdfs://lambda-pluralsight:9000/spark/checkpoint"
    }

    // setup spark context
    val sc = SparkContext.getOrCreate(conf)
    sc.setCheckpointDir(checkpointDir)
    sc
  }

  def getSQLContext(sc: SparkContext) = {
    val sQLContext = SQLContext.getOrCreate(sc)
    sQLContext
  }

  def getStreamingContext(streamingApp: (SparkContext, Duration)=>StreamingContext, sc: SparkContext, bd: Duration) = {
    val creatingFunc = () => streamingApp(sc,bd)
    val ssc = sc.getCheckpointDir match {
      case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir,creatingFunc, sc.hadoopConfiguration,createOnError = true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }
    sc.getCheckpointDir.foreach(cp => ssc.checkpoint(cp))
    ssc
  }
}
