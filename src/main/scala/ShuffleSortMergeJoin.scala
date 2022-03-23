import org.apache.spark.SparkConf

object ShuffleSortMergeJoin {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("ShuffleSortMergeJoin").setMaster("local[*]")

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


    //this property is set because spark internally usage autoBroadcastJoin whenever possible.
    // We set it false to see ShuffleSortMergeJoin
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)

    orderDf.join(customerDf,customerDf("customer_id") === orderDf("order_customer_id")).show()

   // scala.io.StdIn.readLine()
    spark.stop()

  }

}
