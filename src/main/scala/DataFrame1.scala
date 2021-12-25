import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DataFrame1 {
  def main(args: Array[String]): Unit = {

    //val spark = SparkSession.builder().appName("abc").master("local[*]").getOrCreate()

    val conf = new SparkConf()

    conf.set("spark.app.name","chetan")
    conf.set("spark.master","local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val ordersDf = spark
      .read
      .option("header",true)
      .option("inferSchema",true)
      .csv("/Users/chetandeore/Documents/Study/BigData/Spark/orders.csv")


    ordersDf.foreach(x => {
      print(x)
    })

    val groupedOrdersDf = ordersDf.repartition(4)
      .where("order_status == 'CLOSED'")  //.where(ordersDf("order_status") === "CLOSED")
      .where("order_customer_id > 1000")
      .select("order_customer_id","order_status")

    groupedOrdersDf.foreach(x => {
        print(x)
    })

    //groupedOrdersDf.show(10)

    ordersDf.printSchema()

    scala.io.StdIn.readLine()

    spark.stop()



  }
}





