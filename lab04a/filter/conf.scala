import org.apache.spark.sql.SparkSession

object Conf {
  private val confTopicName: (String, String) = ("spark.filter.topic_name","lab04_input_data")
  private val confOffset:    (String, String) = ("spark.filter.offset","earliest")
  private val confOutputDir: (String, String) = ("spark.filter.output_dir_prefix","visits")

  def getConf(spark: SparkSession, confName:(String, String) ): String = {
    spark.sparkContext.getConf.getOption(confName._1) match {
      case Some(x) => {
        println(confName._1 + ":" + x)
        x
      }
      case None => confName._2
    }
  }

  def getTopicName(spark: SparkSession): String = getConf(spark, confTopicName)
  def getOffset   (spark: SparkSession): String = getConf(spark, confOffset)
  def getOutputDir(spark: SparkSession): String = getConf(spark, confOutputDir)
}
