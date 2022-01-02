import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SparkSession}

object AmbiguousError {

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

    //val joinCondition: Column = ordersDf.col("customer_id") === customersDF.col("customer_id")

    val joinType = "outer"

   // val joinedData = customersDF.join(ordersDf,joinCondition,joinType)

    //select the repeated column
    /*joinedData.select("order_id","customer_id").show()*/
    //Reference 'customer_id' is ambiguous, could be: customer_id, customer_id

    //1st option is to rename the column

    /*val newOrdersDf = ordersDf.withColumnRenamed("customer_id","cust_id")

    val joinCondition: Column = newOrdersDf.col("cust_id") === customersDF.col("customer_id")

   val  joinedData = customersDF.join(newOrdersDf,joinCondition,joinType)

    joinedData.select("order_id","customer_id").show()*/


    //OR
    //2nd option is to drop that column
    val joinCondition: Column = ordersDf.col("customer_id") === customersDF.col("customer_id")

    val  joinedData = customersDF.join(ordersDf,joinCondition,joinType)
      .drop(ordersDf.col("customer_id"))
      .select("order_id","customer_id").show()


    spark.stop()

  }
}
