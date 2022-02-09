import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class order(order_id: Int,order_date: String,order_customer_id: Int ,order_status : String)


object TestClass {

  def function(a: Int, b: Int)(f:(Int,Int) => Int) = {
    f(a,b)
  }

  def getTheFileCount(spark : SparkSession) = {


    val inputFile = spark.read.format("csv")
      .option("path","/Users/chetandeore/Documents/Study/BigData/Week12-Apache\\ Spark\\ -\\ Structured\\ API\\ Part-2/orders-201025-223502.csv")
      .option("inferSchema", true)
      .option("header",true)
      .load()

    val count = inputFile.count()

    count

  }


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.app.name", "testDsAndDf")
    sparkConf.set("spark.master","local[*]")

    val spark = SparkSession.builder().config(sparkConf)
      .getOrCreate()

    val count = getTheFileCount(spark)

    spark.stop()

    println(count)

   val c = function(4,5)((x,y) => x*y)

    println(c)

    def fun2(x:Int) = (y :Int) => x*y

    val mul = fun2(5)
    println(mul(5))
  }



}
