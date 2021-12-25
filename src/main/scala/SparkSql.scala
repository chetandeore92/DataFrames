import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSql {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()

    sparkConf.set("spark.master","local[*]")
    sparkConf.set("spark.app.name","Spark SQL learning")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val ordersDf = spark.read
      .format("csv")
      .option("path","/Users/chetandeore/Documents/Study/BigData/Week11-Apache Spark - Structured API Part-1/orders-201019-002101.csv")
      .option("inferSchema",true)
      .option("header",true)
      .load

    //ordersDf.show()

    ordersDf.createOrReplaceTempView("orders")

   // spark.sql("select * from orders").show()

    //spark.sql("select order_status, count(*) as status_count from orders group by order_status order by status_count desc").show()

    spark.sql("select order_customer_id, count(*) as orders_count from  orders where order_status = 'CLOSED' group by order_customer_id order by orders_count desc").show()

    spark.stop()
  }
}
