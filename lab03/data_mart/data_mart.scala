import scala.language.postfixOps
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object data_mart extends App {
  val spark = new SparkSession.Builder()
    .appName("lab03")
    .master("yarn-client")
    //.config("spark.jars", "elasticsearch-spark-20_2.11-6.8.9.jar,postgresql-42.2.12.jar,spark-cassandra-connector_2.11-2.4.3.jar")
    //.enableHiveSupport()
    .getOrCreate()

  // Cassandra - Read

  spark.conf.set("spark.cassandra.connection.host", "10.0.0.31")
  spark.conf.set("spark.cassandra.connection.port", "9042")
  spark.conf.set("spark.cassandra.output.consistency.level", "ANY")
  spark.conf.set("spark.cassandra.input.consistency.level", "ONE")


  val tableOpts = Map("table" -> "clients", "keyspace" -> "labdata")
  val users = spark
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(tableOpts)
    .load()
    .withColumn("age_cat", when(col("age") >= 18 && col("age") < 25, "18-24")
    .when(col("age") >= 25 && col("age") < 35, "25-34")
    .when(col("age") >= 35 && col("age") < 45, "35-44")
    .when(col("age") >= 45 && col("age") < 55, "45-54")
    .when(col("age") >= 55, ">=55")).select("uid", "gender", "age_cat").as("users")

  //ElasticSearch - Read

  val esOptions =
    Map(
      "es.nodes" -> "10.0.0.31:9200",
      "es.batch.write.refresh" -> "false",
      "es.nodes.wan.only" -> "true"
    )
  val visits = spark.read.format("org.elasticsearch.spark.sql").options(esOptions).load("visits").select("uid", "category")
  val shop = visits.filter(col("uid") !== "null")
                   .groupBy("uid")
                   .pivot("category")
                   .agg(count(lit(1)).alias("count"))
                   .na.fill(0)
  val columnnames = shop.columns
  val columnnames2 = columnnames.map(x => "shop_" + x.toLowerCase().replace("-", "_"))
  val shops = shop.toDF(columnnames2: _*).withColumnRenamed("shop_uid", "uid").as("shop")

  //PostgreSql - Read

  val jdbcPgUrlIn = "jdbc:postgresql://10.0.0.31:5432/labdata"
  val jdbcDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://10.0.0.31:5432/labdata")
    .option("user", "vladimir_vasev")
    .option("password", "q9i6DwJL")
    .option("dbtable", "domain_cats")
    .load().as("domains")

  //HDFS - Read

  val hdfsDf = spark.read.json("/labs/laba03/weblogs.json")
  val logs = hdfsDf.select("uid", "visits.url").withColumn("flat_url", explode(col("url")))
    .withColumn("host", ltrim(callUDF("parse_url", col("flat_url"), lit("HOST")), "www.")).as("logs")

  val web_df1 = logs.select("uid", "host").join(broadcast(jdbcDF), col("logs.host") === col("domains.domain"), "inner")
  val web_df2 = web_df1.groupBy("uid").pivot("category").agg(count(lit(1)).alias("count")).na.fill(0)
  val web_dfcols = web_df2.columns
  val web_dfcols2 = web_dfcols.map(x => "web_" + x.toLowerCase().replace("-", "_"))
  val web_sites = web_df2.toDF(web_dfcols2: _*).withColumnRenamed("web_uid", "uid").as("web_sites")
  val result = users.join(web_sites,Seq("uid"), "left").join(shops,Seq("uid"), "left").na.fill(0)

  // Write PostgreSQL
  val jdbcUrlOut = "jdbc:postgresql://10.0.0.31:5432/"
  result.write
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", jdbcUrlOut)
    .option("user", "vladimir_vasev")
    .option("password", "q9i6DwJL")
    .option("dbtable", "clients")
    .mode("overwrite").save

}
