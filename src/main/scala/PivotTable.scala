import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object PivotTable {

  def main(args: Array[String]): Unit = {

    //static month list to avoid distinct calculation from the dataset
    val monthList = List("January", "February",
      "March", "April", "May", "June", "July",
      "August", "September", "October",
      "November", "December")

    val sparkConf = new SparkConf()
    sparkConf.set("spark.master", "local[*]")
    sparkConf.set("spark.app.name", "register Udf")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()


    val bigLog = spark.read
      .format("csv")
      .option("header", true)
      .load("/Users/chetandeore/Documents/Study/BigData/Week12-Apache Spark - Structured API Part-2/biglog-201105-152517.txt")

    //bigLog.show()

    bigLog.createOrReplaceTempView("biglog")

    /*spark.sql("select level, date_format(datetime,'MMMM') as month from biglog")
      .groupBy("level")
      .pivot("month")
      .count()
      .show()*/

    //horizontal grouping on level column
    //Pivot is on month column with static list
    spark.sql("select level, date_format(datetime,'MMMM') as month from biglog")
      .groupBy("level")
      .pivot("month", monthList)
      .count()
      .show()


    spark.stop()
  }

}
