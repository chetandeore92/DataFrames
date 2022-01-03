import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SparkSession }
import org.apache.spark.sql.functions._

object BroadcastJoin {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()

    sparkConf.set("spark.master","local[*]")
    sparkConf.set("spark.app.name","register Udf")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

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

    //disable autoBroadcaseJoinThreshold

    spark.sql("SET spark.auto.autoBroadcastJoinThreshold=-1")

    val joinCondition : Column = customersDF.col("customer_id") === ordersDf.col("customer_id")

    val joinType = "inner" //=> outer,left,right

    ordersDf.join(broadcast(customersDF),joinCondition,joinType).show()

    scala.io.StdIn.readLine()
    spark.stop()


  }

}
