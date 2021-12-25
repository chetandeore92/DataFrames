import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object StructTypeSchema {

  def main(args: Array[String]): Unit = {

  val sparkConf = new SparkConf()

  sparkConf.set("spark.master","local[9]")

  val spark = SparkSession.builder()
    .config(sparkConf).getOrCreate()

    val orderSchema = StructType(List(
      StructField("orderid",IntegerType),
      StructField("orderdate",TimestampType),
      StructField("customerid",IntegerType),
      StructField("status",StringType)
    ))

    //this should exact same as header
    val orderSchemaDDl = "order_id Int, order_date String, order_customer_id Int, order_status String"

    val ordersDf = spark.read
      .option("header",true)
      .format("csv")
      .schema(orderSchemaDDl)
      .option("path","/Users/chetandeore/Documents/Study/BigData/Week11-Apache Spark - Structured API Part-1/orders-201019-002101.csv")
      .load

    ordersDf.printSchema()

   val ordersRep =  ordersDf.repartition(3)

    ordersRep.write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("maxRecordsPerFile", 20000)
      .option("path","/Users/chetandeore/Desktop/BigDataOutput")
      .partitionBy("order_status")
      .save()



  }
}
