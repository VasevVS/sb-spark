import scala.language.postfixOps
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object data_mart extends App {

  //spark_activate
  val spark = new SparkSession.Builder()
    .appName("lab03")
    .master("local")
    .config("spark.jars", "elasticsearch-spark-20_2.11-6.8.9.jar,postgresql-42.2.13.jar,spark-cassandra-connector_2.11-2.5.0.jar")
    //.enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  spark.conf.set("spark.cassandra.connection.host", "10.0.0.31:9042")
  spark.conf.set("spark.cassandra.output.consistency.level", "ANY")
  spark.conf.set("spark.cassandra.input.consistency.level", "ONE")

  val tableOpts = Map("table" -> "clients", "keyspace" -> "labdata")
  val users = spark
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(tableOpts)
    .load().withColumn("age_cat", when(col("age") >= 18 && col("age") < 25, "18-24")
    .when(col("age") >= 25 && col("age") < 35, "25-34")
    .when(col("age") >= 35 && col("age") < 45, "35-44")
    .when(col("age") >= 45 && col("age") < 55, "45-54")
    .when(col("age") >= 55, ">=55")).select("uid", "gender", "age_cat").as("users")

  println(users)

}