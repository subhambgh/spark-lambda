package streaming

import config.Settings
import utils.SparkUtils._

object StreamingJob {
  def main(args: Array[String]): Unit = {

    val wlc = Settings.WebLogGen

    val spark = getSparkSession("lambda")

    val inputDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", wlc.bootstrap_server_kafka)
      .option("subscribe", wlc.kafkaTopic)
      .load()

    val trainData = inputDf.selectExpr("CAST(value AS STRING)")

    val consoleOutput = trainData.writeStream
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination()
  }
}
