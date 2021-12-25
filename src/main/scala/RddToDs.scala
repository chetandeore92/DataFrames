import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RddToDs {

  case class Order(order_id: Int,customer_id :Int,status: String)

  // .r and triple quote is to indicate that it is a regex
  val myregex1 = """^(\S+) (\S+)\t(\S+),(\S+)""".r

  def parser(line : String) = {

    line match {
      case myregex1(order_id,date,customer_id,status) =>
        Order(order_id.toInt,customer_id.toInt,status)
    }

  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()

    sparkConf.set("spark.master","local[*]")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val lines = spark.sparkContext.textFile("/Users/chetandeore/Documents/Study/BigData/Week11-Apache Spark - Structured API Part-1/orders_new-201019-002101.csv")

    val rdd = lines.map(parser)

    import spark.implicits._

    val ordersDs =  rdd.toDS()

    //ordersDs.filter(x => x.order_id > 10).select("order_id").show()

   // ordersDs.select("order_id","status").show()

    //ordersDs.select(col("order_id"),column("status")).show()

    //this requires import spark.implicits._
    //ordersDs.select($"order_id",'status).show()

    //expr -> is to convert coln expression to column object
   // ordersDs.select(col("order_id"),expr("concat(status,'_STATUS')")).show(false)

    ordersDs.selectExpr("order_id","concat(status,'_STATUS')").show

    spark.stop()

  }
}
