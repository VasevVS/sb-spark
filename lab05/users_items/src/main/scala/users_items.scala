import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object users_items extends App {
  val spark = SparkSession.builder().getOrCreate()

  import spark.implicits._
  import org.apache.hadoop.fs.{FileSystem, Path}

  val update_val = spark.conf.get("spark.users_items.update")
  val out_path = spark.conf.get("spark.users_items.output_dir")
  val in_path = spark.conf.get("spark.users_items.input_dir")

  val update_mode = if (update_val == "0") "overwrite" else "append"

  //т.к. view.count() = 59996, а buy.count() = 28070, то, чтобы два раза не читать, можно оба кэшировать

  val view = spark.read
    .json(s"$in_path/view")
    .filter('uid.isNotNull)
    .select('uid, 'date, 'item_id)
    .cache()

  val buy = spark.read
    .json(s"$in_path/buy")
    .filter('uid.isNotNull)
    .select('uid, 'date, 'item_id)
    .cache()

  val last_date_view = view
    .groupBy()
    .agg(max('date))
    .first().getString(0).toInt
  val last_date_buy = buy
    .groupBy()
    .agg(max('date))
    .first().getString(0).toInt

  val last_date = if (last_date_buy >= last_date_view) last_date_buy else last_date_view

  val users_view_items = view
    .select('uid, 'item_id)
    .withColumn("item_id", regexp_replace('item_id, "-| ", "_"))
    .withColumn("item_id", upper('item_id))
    .withColumn("item_id", concat(lit("view_"), 'item_id))
    .groupBy('uid)
    .pivot('item_id)
    .agg(count('item_id))

  val users_items = buy
    .select('uid, 'item_id)
    .withColumn("item_id", regexp_replace('item_id, "-| ", "_"))
    .withColumn("item_id", lower('item_id))
    .withColumn("item_id", concat(lit("buy_"), 'item_id))
    .groupBy('uid)
    .pivot('item_id)
    .agg(count('item_id))
    .join(users_view_items, Seq("uid"), "full")
    .na.fill(0)

  if (update_val == "0") {
    users_items
      .write
      .mode("overwrite")
      .parquet(s"$out_path/$last_date")
  }
  else {

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val status = fs.listStatus(new Path(out_path))
    val path = status(0).getPath().getName()
    val users_items_old = spark.read.parquet(s"$out_path/$path")

    val cols1 = users_items_old.columns.toSet
    val cols2 = users_items.columns.toSet
    val total = cols1 ++ cols2



    def expr (myCols: Set[String], allCols: Set[String]) = {
      allCols.toList.map {
        case x if myCols.contains(x) => col(x)
        case x => lit(null).as(x)
      }
    }

    val result = users_items_old
      .select(expr (cols1, total):_*)
      .union(users_items.select(expr (cols2, total):_*))
      .write
      .mode("append")
      .parquet(s"$out_path/$last_date")
  }

  view.unpersist()
  buy.unpersist()
}
