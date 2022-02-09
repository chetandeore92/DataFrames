import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}




object    DataFrame2 {



  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()

    sparkConf.set("spark.app.name","DFtoDS")
    sparkConf.set("spark.master","local[*]")

    val sc = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    /*
    val orderDf : Dataset[Row]= sc.read
      .option("header",true)
      .option("inferSchema",true)
      .csv("/Users/chetandeore/Documents/Study/BigData/Spark/orders.csv")
      */

    //orderDf.filter("order_id > 10").show()

    //orderDf.filter("order_ids > 10").show()

    import sc.implicits._

    val orderDs : Dataset[OrdersData]= sc.read
      .option("header",true)
      .option("inferSchema",true)
      .csv("/Users/chetandeore/Documents/Study/BigData/Spark/orders.csv")
      .as[OrdersData]

    orderDs.filter(x => x.order_id > 10).show()



  }
}
