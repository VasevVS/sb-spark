import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, IndexToString, StringIndexer}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructType, _}

object test extends App {

  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._

  val modelPath = spark.conf.get("spark.test.modelPath")

  //определяем схему для чтения данных из Кафки
  val schema = StructType(List(
    StructField("uid", StringType, nullable = true),
    StructField("visits", ArrayType(StructType(List(
      StructField("timestamp", StringType, nullable = true),
      StructField("url", StringType, nullable = true)))),
      nullable = true)))

  val test =
  //читаем топик из Кафки
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.0.0.31:6667")
      .option("subscribe", "vladimir_vasev")
      .load
      //преобразуем данные для применения в модели
      .select(from_json('value.cast(StringType), schema).alias("value"))
      .select(col("value.uid").alias("uid"), col("value.visits").alias("visits"))
      .select('uid, explode(col("visits.url")).alias("domains"))
      .withColumn("domains", lower(callUDF("parse_url", 'domains, lit("HOST"))))
      .withColumn("domains", regexp_replace('domains, "www.", ""))
      .groupBy('uid)
      .agg(collect_list('domains).alias("domains"))

  //вычитываем модель
  val myModel = PipelineModel.load(modelPath)

  //определяем синк для записи в Кафку с необходимыми проебразованиями батча
  val sink = test
    .writeStream
    .trigger(Trigger.ProcessingTime("30 seconds"))
    .outputMode("update")
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      val res = myModel
        .transform(batchDF)
        .select(to_json(struct(col("uid"), col("label_string").alias("gender_age"))).alias("value"))
      res.write
        .format("kafka")
        .option("kafka.bootstrap.servers", "10.0.0.31:6667")
        .option("topic", "vladimir_vasev_lab07_out")
        .save
    }
  //Поехали!!!
  sink.start()
  spark.stop()
}
