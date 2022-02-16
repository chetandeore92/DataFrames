import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object HiveSupport {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.master","local[*]")
    sparkConf.set("spark.app.name","HiveSupport for metastore")
    sparkConf.set("spark.sql.warehouse.dir","/Users/chetandeore/Documents/Study/warehouse")


    //enable hive support to use hive metastore instead of in-memory metastore
      //add spark hive lib 3.2.0 2.12.15
    val spark = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    ///spark.conf.get("spark.sql.warehouse.dir")

    val ordersdDf = spark.read
      .format("csv")
      .option("path","/Users/chetandeore/Documents/Study/BigData/Week11-Apache Spark - Structured API Part-1/orders-201019-002101.csv")
      .option("header","true")
      .option("inferSchema",true)
      .load

    //val s = ordersdDf.foreach(x => println)

    spark.sql("create database if not exists retail")

    ordersdDf.write
      .format("csv")
      .mode(saveMode = SaveMode.Overwrite)
      .bucketBy(5,"order_customer_id")
      .sortBy("order_customer_id")
      .saveAsTable("retail.orders")

    //list all tables in retail.db database
    spark.catalog.listTables("retail").show()

    spark.catalog.listDatabases().show(false)


    spark.stop()


  }
}
