import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._


object filter {

  // 6 attr
  case class Event(event_type:String, category: String, item_id: String, item_price: Long, uid: String, timestamp: Long)

  def main(args: Array[String]): Unit = {
    val eventSchema: StructType = ScalaReflection.schemaFor[Event].dataType.asInstanceOf[StructType]

    // spark start!!!
    val spark: SparkSession = SparkSession.builder
      .appName("vasevVS_lab04a")
      .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5, org.apache.kafka:kafka-clients:0.10.1.0")
      //   .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val topicName: String  = Conf.getTopicName(spark)
    val offset: String = Conf.getOffset(spark)
    val outputDir: String = Conf.getOutputDir(spark)

    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribePattern", topicName)
      .option("startingOffsets", if(offset.contains("earliest")) offset
      else { "{\"" + topicName + "\":{\"0\":" + offset + "}}"
      } )
      .load()

    df.printSchema()

    val dfJS: DataFrame = df.withColumn("js",col("value").cast("STRING"))
                            .select(from_json(col("js"),eventSchema))
                            .select("jsontostructs(js).*")

    //
    //  val df = spark.read.schema(eventSchema).json("/labs/laba04/visits-g")

    val df1= dfJS.withColumn("date",
      date_format(col("timestamp")
        .divide(1000)
        .cast("timestamp")
        ,"yyyyMMdd"))
      .withColumn("_date",col("date"))

    df1.filter(col("event_type") === "view")
      .write
      .mode("overwrite")
      .partitionBy("_date")
      .json(outputDir + "/view")

    df1.filter(col("event_type") === "buy")
      .write
      .mode("overwrite")
      .partitionBy("_date")
      .json(outputDir + "/buy")
  }
}
