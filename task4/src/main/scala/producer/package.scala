import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

package object producer {
  val servers: String = "127.0.0.1:9092"
  val sourceTopic: String = "sourceTopicV1"
  val stringSerializerName: String = new StringSerializer().getClass.getName
  val customPartitionerClass: String = "producer.SessionIdPartitioner"

  def producerProperties: Properties = {
    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers)
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, stringSerializerName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, stringSerializerName)
    props.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, customPartitionerClass)
    props
  }

}
