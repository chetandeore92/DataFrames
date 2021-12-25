import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSaveAsTable2 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.master","local[*]")
    sparkConf.set("spark.app.name","Save as table in spark warehouse")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val ordersDf = spark.read
      .format("csv")
      .option("path","/Users/chetandeore/Documents/Study/BigData/Week11-Apache Spark - Structured API Part-1/orders-201019-002101.csv")
      .option("header",true)
      .option("inferSchema",true)
      .load()

    ordersDf.createOrReplaceTempView("orders")

    val orders = spark.sql("select order_status, count(*) as status_count from orders group by order_status order by status_count desc")

    orders.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .saveAsTable("orders")

    spark.stop()

  }
}
