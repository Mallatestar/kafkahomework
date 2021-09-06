package consumer

import java.nio.charset.Charset
import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.io.Source

class OffsetManager(private val workDir: String, private val topic:String, private val availablePartitions: Iterable[Int]) {

  private def initWorkDir(): Unit = {
    val rootPath = Paths.get(workDir, topic)
    Files.createDirectory(rootPath)
    availablePartitions.foreach(x => Files.createFile(Paths.get(s"${rootPath.toString}/partition$x.txt")))
  }

  def registerMessage(partition: Int, offset: Long): Unit = {
    Files.write(Paths.get(
      s"$workDir/$topic/partition$partition.txt"),
      (offset + System.lineSeparator()).getBytes(Charset.defaultCharset()),
      StandardOpenOption.APPEND
    )
  }

  def getLastOffsetForPartition(partition:Int): Long = {
    val source = Source.fromFile(s"$workDir/$topic/partition$partition.txt")
    val storedValueOption = source
      .getLines()
      .toList
      .reverse
      .headOption
    storedValueOption match {
      case Some(x: String) => x.toLong
      case None => 0L
    }
  }

}

object OffsetManager {
  def apply(managerWorkDir: String, topic:String, availablePartitions: Iterable[Int]): OffsetManager = {
    val manager = new OffsetManager(managerWorkDir, topic, availablePartitions)
    if (!Files.exists(Paths.get(managerWorkDir, topic))) manager.initWorkDir()
    manager
  }

}
