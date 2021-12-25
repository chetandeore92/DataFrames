import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SimpleAggregation {

  def main(args: Array[String]): Unit = {

   Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf = new SparkConf()

    sparkConf.set("spark.master","local[*]")
    sparkConf.set("spark.app.name","SimpleAggregation")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val invoice = spark.read
      .format("csv")
      .option("header",true)
      .option("path","/Users/chetandeore/Documents/Study/BigData/Week12-Apache Spark - Structured API Part-2/order_data-201025-223502.csv")
      .load()

    //Coln object expression

    invoice.select(
      count("*") as ("TotalRows"),
      sum("Quantity") as ("TotalQty"),
      avg("UnitPrice") as ("AvgUnitPrice"),
      countDistinct("InvoiceNo") as ("uniqueInvoiceCount")
    ).show()

    //String expression

    invoice.selectExpr(
      "count(*) as TotalRows",
      "sum(Quantity) as TotalQty",
      "avg(UnitPrice) as AvgUnitPrice",
      "count(distinct(InvoiceNo)) as uniqueInvoiceCount"
    ).show()

    //spark SQL
    invoice.createOrReplaceTempView("invoice")

    spark.sql("select count(*) as TotalRows, sum(Quantity) as TotalQty, avg(UnitPrice) as AvgUnitPrice, count(distinct(InvoiceNo)) as uniqueInvoiceCount from invoice").show()

    //val invoiceWithHeader = invoice.toDF("InvoiceNo","StockCode","Description","Quantity", "InvoiceDate","UnitPrice","CustomerID","Country")

    //invoiceWithHeader.show()

    spark.stop()

  }

}
