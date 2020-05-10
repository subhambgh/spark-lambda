package producer

import java.util.Properties

import config.Settings
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}

import scala.io.Source

object Producer extends App {

  val wlc = Settings.WebLogGen

  val topic = wlc.kafkaTopic

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, wlc.bootstrap_server_kafka)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ACKS_CONFIG, "all")
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer")

  val kafkaProducer: Producer[Nothing, String] = new KafkaProducer[Nothing, String](props)
  println(kafkaProducer.partitionsFor(topic))

  val readFile = Source
    .fromFile(wlc.filePath)
    .getLines

  readFile.foreach(line => {
    kafkaProducer.send(new ProducerRecord(topic, line))
  })

  kafkaProducer.close()
}
