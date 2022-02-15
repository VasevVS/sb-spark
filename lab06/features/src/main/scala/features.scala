import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType,IntegerType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object features extends App {

  val spark = SparkSession
    .builder()
    .appName("VasevVS Lab06")
    .getOrCreate()


  val webDF = spark.read.parquet("hdfs:///labs/laba03/weblogs.parquet")
    .repartition(200)
    .withColumn("visit", explode(col("visits")))
    .select(col("uid"), col("visit.url").alias("url"), col("visit.timestamp").as("ts"))
    .selectExpr("uid", "ts", "regexp_replace(lower(parse_url(url,'HOST')),'www.','') as domain").where("domain is not null")
    .cache()

  val domainStat = webDF.groupBy("domain").agg(count(lit(1)).alias("cnt"))

  val windowSpecCnt = Window.orderBy(col("cnt").desc)

  val top1000domain = domainStat.withColumn("rn",row_number().over(windowSpecCnt)).filter(col("rn")<=1000).drop("cnt", "rn").withColumnRenamed("domain","topdomain").cache()

  val webDF_joined = webDF.join(top1000domain, webDF.col("domain") === top1000domain.col("topdomain") ,joinType = "left_outer")

  val uidStat = webDF_joined.groupBy(col("uid")).pivot("topdomain").agg(count(lit(1)).alias("cnt"))

  val colNames = uidStat.drop("uid", "null").schema.fieldNames

  val domain_Expr = "array(" + colNames.map (x => "`"+ x +"`").mkString(",") + ") as domain_features"

  val domain_features_df = uidStat.drop("null").na.fill(0).selectExpr("uid", domain_Expr)

  val webDF_ts = webDF.withColumn("ts", (col("ts")/1000-10800).cast(LongType))

  val webDF_time_df = webDF_ts.
    withColumn("hr_name", concat(lit("web_hour_"), from_unixtime(col("ts"),"HH").cast(IntegerType))).
    withColumn("hr", from_unixtime(col("ts"),"HH").cast(IntegerType)).
    withColumn("wd", concat(lit("web_day_"), lower(from_unixtime(col("ts"),"E"))))


  val web_hour_features_df = webDF_time_df.groupBy("uid").pivot("hr_name").count().na.fill(0)

  val web_day_features_df = webDF_time_df.groupBy("uid").pivot("wd").count().na.fill(0)

  val web_fraction_df = webDF_time_df.groupBy("uid").
    agg(count(lit(1)).alias("cnt"),
      count(when(col("hr")>=9 && col("hr")<=18, lit(1))).alias("wh"),
      count(when(col("hr")>=18 && col("hr")<=23, lit(1))).alias("eh")
    ).withColumn("web_fraction_work_hours", col("wh") / col("cnt")).
    withColumn("web_fraction_evening_hours", col("eh")/ col("cnt")).select(col("uid"), col("web_fraction_work_hours"), col("web_fraction_evening_hours"))


  val users_items_df = spark.read.parquet("/user/vladimir.vasev/users-items/20200429")

  val result = domain_features_df.join(web_hour_features_df, usingColumn = "uid").join(web_day_features_df, usingColumn = "uid").
    join(web_fraction_df, usingColumn = "uid").join(users_items_df, usingColumns=Seq("uid"), joinType = "left_outer").na.fill(0)

  result.repartition(1).write.mode("overwrite").parquet("/user/vladimir.vasev/features")

}
