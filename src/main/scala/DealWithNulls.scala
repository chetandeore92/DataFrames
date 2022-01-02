import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Column, SparkSession}

object DealWithNulls {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()

    sparkConf.set("spark.master","local[*]")
    sparkConf.set("spark.app.name","AmbiguousError")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val ordersDf = spark.read
      .format("csv")
      .option("path","/Users/chetandeore/Documents/Study/BigData/Week12-Apache Spark - Structured API Part-2/orders")
      .option("header",true)
      .load()

    //ordersDf.show()

    val customersDF = spark.read
      .format("csv")
      .option("path","/Users/chetandeore/Documents/Study/BigData/Week12-Apache Spark - Structured API Part-2/customers")
      .option("header",true)
      .load()

    //customersDF.show()

    val joinCondition: Column = ordersDf.col("customer_id") === customersDF.col("customer_id")

    val joinType = "outer"

    //coalesce is to replace nulls with different value
    val joinedData = ordersDf.join(customersDF,joinCondition,joinType)
      .withColumn("order_id",expr("coalesce(order_id,-1)"))
      .sort("order_id")
      .show()



    spark.stop()


  }
}
