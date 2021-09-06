package consumer

import com.google.gson.Gson
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.slf4j.{Logger, LoggerFactory}

import java.time.Duration
import scala.jdk.CollectionConverters.{IterableHasAsScala, ListHasAsScala, SeqHasAsJava}

object RawDataConsumer {
  val logger: Logger = LoggerFactory.getLogger(getClass)
  val gson = new Gson()

  def main(args: Array[String]): Unit = {
    val producer = new KafkaProducer[String, String](producerProperties)
    val consumer = new KafkaConsumer[String, String](consumerProperties)

    val availablePartitions = consumer
      .partitionsFor(sourceTopic)
      .asScala
      .map(_.partition())
      .toList

    val offsetManager: OffsetManager = OffsetManager(managerWorkDir, sourceTopic, availablePartitions)
    val partitions = availablePartitions.map(new TopicPartition(sourceTopic, _))
    consumer.assign(partitions.asJava)
    partitions.foreach(partition =>
      consumer.seek(partition, offsetManager.getLastOffsetForPartition(partition.partition())))

    while (true) {
      consumer
        .poll(Duration.ofMillis(1000))
        .asScala
        .map(record => {
          val countryCode = getCountryCode(record.key())
          val splitMessage = record.value().split(",")
          RichClick(record.key().toInt,
            splitMessage(0),
            splitMessage(1).toInt,
            splitMessage(2),
            countryCode)
        })
        .map(click => {
          val topic = if (click.category == "0") errorTopic else resultTopic
          new ProducerRecord[String, String](topic, click.sessionID.toString, gson.toJson(click))
        })
        .foreach(record => {
          val result = producer.send(record)
          val metaData = result.get()
          offsetManager.registerMessage(metaData.partition(), metaData.offset())
        })
    }

  }


}



