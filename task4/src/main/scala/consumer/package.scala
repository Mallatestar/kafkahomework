import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import java.util.Properties

package object consumer {
  val servers: String = "127.0.0.1:9092"
  val deserializerClass: String = new StringDeserializer().getClass.getName
  val stringSerializerName: String = new StringSerializer().getClass.getName
  val sourceTopic: String = "sourceTopicV1"
  val errorTopic: String = "errorTopicV1"
  val resultTopic: String = "RichTopicV1"
  val acknowledgments: String = "all"
  val autoCommit: String = "false"
  val lingerMs: String = "1"
  val retries: String = "0"
  val countryCodes: List[String] = List("US", "GB", "DE", "UA", "AF", "BE", "CN", "FI", "JP", "NE")
  val invalidCountryCode: String = "UNAVAILABLE"
  val managerWorkDir: String = "/tmp"

  def consumerProperties: Properties = {
    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers)
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializerClass)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializerClass)
    properties
  }

  def producerProperties: Properties = {
    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers)
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, stringSerializerName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, stringSerializerName)

    props
  }

  def getCountryCode(id: String): String = {
    Option(id) match {
      case Some(x: String) => countryCodes(x.toInt % countryCodes.size)
      case None => invalidCountryCode
    }
  }

  case class RichClick(sessionID: Int, timestamp: String, itemID: Int, category: String, country: String)
}
