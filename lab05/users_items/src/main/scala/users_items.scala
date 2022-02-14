import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object users_items extends App {

  val spark = SparkSession.builder.master("local")
                          .appName("VasevVS_lab5")
                          .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  // params
  val visitsDir = spark.sparkContext.getConf.getOption("spark.users_items.input_dir").getOrElse("")
  val modeType = spark.sparkContext.getConf.getOption("spark.users_items.update").getOrElse(1)
  val outputDir = spark.sparkContext.getConf.getOption("spark.users_items.output_dir").getOrElse("")

  // read json
  val dfBuys = spark.read.json(s"$visitsDir/buy").withColumn("itype", lit("buy_"))
  val dfViews = spark.read.json(s"$visitsDir/view").withColumn("itype", lit("view_"))

  //
  val dfUsersItems = dfBuys
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

  dfUsersItems
    .write
    .format("parquet")
    .mode(if(modeType==1) "append" else "overwrite")
    .save(s"$outputDir/$datePrefix")

  spark.stop
}