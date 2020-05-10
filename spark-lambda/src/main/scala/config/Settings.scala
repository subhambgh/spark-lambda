package config

import com.typesafe.config.ConfigFactory

object Settings {
  private val config = ConfigFactory.load()

  object WebLogGen {
    private val weblogGen = config.getConfig("crimeRecords")
    lazy val filePath = weblogGen.getString("file_path")

    //kafka
    lazy val kafkaTopic = weblogGen.getString("kafka_topic")
    lazy val bootstrap_server_kafka = weblogGen.getString("bootstrap_server_kafka")

  }
}
