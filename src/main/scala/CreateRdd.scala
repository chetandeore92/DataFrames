import org.apache.spark.sql.SparkSession

case class order1(order_id : String, order_status : String)

object CreateRdd {

  def main(args: Array[String]): Unit = {

    //val sc = new SparkContext("local[*]","testRdd")

    val spark = SparkSession.builder().appName("testRdd").master("local[*]").getOrCreate()

    val inputFile = spark.sparkContext.textFile("/Users/chetandeore/Documents/Study/BigData/Week12-Apache\\ Spark\\ -\\ Structured\\ API\\ Part-2/orders-201025-223502.csv")


    //inputFile.collect().foreach(println)

    val processedRdd = inputFile.map(x => {
      val fields = x.split(",")
      (fields(0),fields(3))
    })

    import spark.implicits._

    val finalDf = processedRdd.toDF("order_id","order_status")

    //finalDf.collect().foreach(println)

    finalDf.as[order1].take(20).foreach(println)

    spark.stop()
  }

}
