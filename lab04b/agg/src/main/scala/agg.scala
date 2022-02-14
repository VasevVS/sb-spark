// !!!!!
//spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.{from_json, window, sum, count, min, max, when, to_json,struct}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object agg extends App {

  case class InputJson(event_type: String, category: String,
                       item_id: String, item_price: Long,
                       uid: String, timestamp: Long)

  implicit val spark: SparkSession = SparkSession.builder.
    appName("vladimir.vasev.lab04b.agg").getOrCreate()


  import spark.implicits._

  val bootstrapServer = "spark-master-1:6667"
  //val bootstrapServer1 = "10.0.0.31:6667"
  val topicNameIn = "vladimir_vasev"
  val topicNameOut = "vladimir_vasev_lab04b_out"

  val schema = ScalaReflection.schemaFor[InputJson].dataType.asInstanceOf[StructType]

  val dfStream = spark.readStream.
    format("kafka").
    option("kafka.bootstrap.servers", bootstrapServer).
    option("subscribe", topicNameIn).
    load()

  val dfAgg = dfStream.select(from_json($"value" cast "string", schema) as "record").
    select($"record.*").groupBy(window(($"timestamp" / 1000).
    cast("timestamp"), "1 hour")).agg(
    min("window.start").cast("long") as "start_ts",
    max("window.end").cast("long") as "end_ts",
    sum(when($"event_type" === "buy", $"item_price")) as "revenue",
    count(when($"uid" isNotNull, $"event_type")) as "visitors",
    count(when($"event_type" === "buy", $"event_type")) as "purchases").
    withColumn("aov", $"revenue" / $"purchases").drop("window")

  dfAgg.select(to_json(struct("*")) as "value").writeStream.
    format("kafka").
    //option("kafka.bootstrap.servers", bootstrapServer1).
    option("kafka.bootstrap.servers", bootstrapServer).
    option("topic", topicNameOut).
    option("checkpointLocation", "checkpointdir").
    outputMode(OutputMode.Update()).
    trigger(Trigger.ProcessingTime(5000)).
    start().awaitTermination()
}
