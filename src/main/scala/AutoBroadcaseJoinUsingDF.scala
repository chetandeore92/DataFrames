import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.broadcast

object AutoBroadcaseJoinUsingDF {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("AutoBroadcaseJoinUsingDF")
    val spark = GetSparkInstance.get(sparkConf)

    val orderDf = spark.read.format("csv")
      .option("path","/Users/chetandeore/Documents/Study/BigData/Spark/23082020/orders.csv")
      .option("header",true)
      .load()

    //orderDf.show()

    val customerDf = spark.read.format("csv")
      .option("path","/Users/chetandeore/Documents/Study/BigData/Spark/23082020/customers.csv")
      .option("header",true)
      .load()

    orderDf.join(broadcast(customerDf),customerDf("customer_id") === orderDf("order_customer_id")).show()

    //scala.io.StdIn.readLine()

    spark.stop()


  }
}
