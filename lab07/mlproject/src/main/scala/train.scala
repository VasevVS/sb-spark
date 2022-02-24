import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, IndexToString}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.types.{StringType, StructType, _}


object train extends App {
  val spark = SparkSession.builder().getOrCreate()

  import spark.implicits._

  val trainInputPath = spark.conf.get("spark.train.trainInputPath")
  val modelPath = spark.conf.get("spark.train.modelPath")

  val schema = StructType(List(
    StructField("uid", StringType, nullable = true),
    StructField("visits", ArrayType(StructType(List(
      StructField("timestamp", StringType, nullable = true),
      StructField("url", StringType, nullable = true)))),
      nullable = true)))

  val weblogs = spark.read
    .format("json")
    .schema(schema)
    .option("inferSchema", "false")
    .load(trainInputPath)
    .filter('uid.isNotNull)
    .select('uid, 'gender_age, explode('visits).alias("visits"))
    .select('uid, col("visits.url").alias("domains"), 'gender_age)
    .withColumn("domains", lower(callUDF("parse_url", 'domains, lit("HOST"))))
    .withColumn("domains", regexp_replace('domains, "www.", ""))
    .groupBy('uid, 'gender_age)
    .agg(collect_list('domains).alias("domains"))

  val cv = new CountVectorizer()
    .setInputCol("domains")
    .setOutputCol("features")

  val indexer = new StringIndexer()
    .setInputCol("gender_age")
    .setOutputCol("label")
    .fit(weblogs)

  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.001)

  val is = new IndexToString()
    .setInputCol("prediction")
    .setLabels(indexer.labels)
    .setOutputCol("label_string")

  val pipeline = new Pipeline()
    .setStages(Array(cv, indexer, lr, is))

  pipeline
    .fit(weblogs)
    .write
    .overwrite()
    .save(modelPath)

  spark.stop()

}
