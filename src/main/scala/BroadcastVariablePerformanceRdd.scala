import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object BroadcastVariablePerformanceRdd {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("BroadcastVariablePerformanceRdd").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val largeFile = spark.sparkContext
      .textFile("/Users/chetandeore/Documents/Study/BigData/Spark/orders.csv")
      .mapPartitionsWithIndex{(idx, iter) => if (idx == 0) iter.drop(1) else iter }

    val largeRdd = largeFile.map(x=>
    {
      val fields = x.split(",")
      (fields(3),fields(2))
    })

    val map = Map("CLOSED"->0,
      "COMPLETE"->1,
      "PENDING_PAYMENT"->2,
    "PROCESSING"->3,
    "SUSPECTED_FRAUD"->4,
    "PAYMENT_REVIEW"->5,
    "PENDING"->6,
    "ON_HOLD"->7,
    "CANCELED"->8)

    val bcast = spark.sparkContext.broadcast(map)

    val finalJoin = largeRdd.map(x => (x._1,x._2,bcast.value(x._1)))

    finalJoin.saveAsTextFile("/Users/chetandeore/Documents/Study/BigData/Spark/Output4")

    spark.stop()

  }

}
