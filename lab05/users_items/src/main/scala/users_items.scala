import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext._


object users_items extends App {

  val spark = SparkSession.builder.appName("VasevVSlab5").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val visitsDir = spark.sparkContext.getConf.getOption("spark.users_items.input_dir").getOrElse("")
  val modeType = spark.sparkContext.getConf.getOption("spark.users_items.update").getOrElse(1)
  val outputDir = spark.sparkContext.getConf.getOption("spark.users_items.output_dir").getOrElse("")

  val dfBuys = spark.read.json(s"$visitsDir/buy").withColumn("itype", lit("buy_"))
  val dfViews = spark.read.json(s"$visitsDir/view").withColumn("itype", lit("view_"))

  val dfUserItems = dfBuys
    .union(dfViews)
    .withColumn("item_type",
      concat('itype, regexp_replace(lower('item_id),"""[\s+\-+]""","_" )
      )
    )
    .withColumn("one", lit(1))
    .groupBy('uid)
    .pivot('item_type)
    .agg(sum('one))
    .na.fill(0)

  val datePrefix = dfBuys.union(dfViews)
    .agg(max("date"))
    .first()
    .mkString

  dfUserItems
    .write
    .format("parquet")
    .mode(if(modeType==1) "append" else "overwrite")
    .save(s"$outputDir/$datePrefix")

  spark.stop
}