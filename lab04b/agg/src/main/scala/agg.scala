import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object agg {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("agg").getOrCreate()
    import spark.implicits._

    val kafkaReadParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "subscribe" -> "yuriy_osychenko"
    )

    val rawKafkaStream = spark.readStream.format("kafka").options(kafkaReadParams).load

    val jsonDataFromKafka = rawKafkaStream.select('value.cast("string")).as[String]

    val datasetRowSchema = StructType(
      List(
        StructField("event_type", StringType),
        StructField("category", StringType),
        StructField("item_id", StringType),
        StructField("item_price", IntegerType),
        StructField("uid", StringType),
        StructField("timestamp", LongType)
      )
    )

    val streamDataset = jsonDataFromKafka
      .select(from_json('value, datasetRowSchema) as "data")
      .select("data.*")
      .withColumn("date_time_ts", from_unixtime('timestamp / lit(1000), "yyyy-MM-dd HH:mm:ss"))

    val groupedStreamDataset = streamDataset
      .groupBy(window('date_time_ts, "1 hours"))
      .agg(
        sum(when('event_type === "buy", 'item_price).otherwise(0)).as("revenue"),
        sum(when('uid.isNotNull, 1).otherwise(0)).as("visitors"),
        sum(when('event_type === "buy", 1).otherwise(0)).as("purchases")
      )
      .withColumn("aov", 'revenue / 'purchases)

    val JSONFormattedGroupedStreamDataset = groupedStreamDataset
      .select("*", "window.start", "window.end")
      .withColumn("start_ts", unix_timestamp('start))
      .withColumn("end_ts", unix_timestamp('end))
      .drop("window", "start", "end")
      .toJSON

    val kafkaParamsWrite = Map(
      "kafka.bootstrap.servers" -> "spark-master-1:6667",
      "topic" -> "yuriy_osychenko_lab04b_out",
      "checkpointLocation" -> "/user/yuriy.osychenko/checkpoints"
    )

    JSONFormattedGroupedStreamDataset.writeStream
      .format("kafka")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .options(kafkaParamsWrite)
      .start

    spark.stop
  }
}
