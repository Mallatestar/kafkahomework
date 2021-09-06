package producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source
import scala.util.Using

object RawDataProducer {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    Using.Manager {
      use =>
        val producer = use(new KafkaProducer[String, String](producerProperties))
        val source = use(Source.fromFile(args.head))

        source
          .getLines()
          .foreach(row => {
            val record = new ProducerRecord[String, String](sourceTopic, getKey(row), parseClick(row))
            val result = producer.send(record)
            val meta = result.get()
            logger.info(s"Message {$row} has been sent into ${meta.partition()} partition")
          })
    }
  }

  private def getKey(string: String): String = {
    string.split(",").headOption match {
      case Some(x: String) => x
      case None => "0"
    }
  }

  private def parseClick(string: String): String = {
    val headIndex = string.indexOf(",") + 1
    string.substring(headIndex)
  }
}
