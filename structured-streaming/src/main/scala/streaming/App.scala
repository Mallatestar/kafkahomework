package streaming

import com.google.gson.Gson
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}

object App {
  /**
   * @param args
   * 0 -- checkpoint location for streaming
   * 1 -- bootstrap servers for kafka
   * 2 -- topic to read
   * 3 -- path to save results
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0")
      .config("spark.sql.streaming.checkpointLocation", args(0))
      .getOrCreate()
    import spark.implicits._
    val gson = new Gson()
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", args(1))
      .option("subscribe", args(2))
      .option("startingOffsets", "earliest")
      .load()
      .select($"value")
      .as[String]
      .map(row2RichClick(_, gson))
      .withColumn("datePartitioner", to_date(col("timestamp")))
      .writeStream
      .partitionBy("datePartitioner", "country")
      .format("json")
      .start(args(3))
      .awaitTermination()
  }

  private def row2RichClick(string: String, gson: Gson) : RichClick = {
    gson.fromJson(string, RichClick.getClass)
  }
  case class RichClick(sessionID: Int, timestamp: String, itemID: Int, category: String, country: String)
}
