package producer

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

import java.util

class SessionIdPartitioner extends Partitioner{
  override def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
    val partitionsCount = cluster.availablePartitionsForTopic(topic).size()
    Option(key) match {
      case Some(x: String) => x.toInt % partitionsCount
      case _ => partitionsCount
    }
  }

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {}
}
