import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object GroupedAggregate {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    conf.set("spark.app.name", "GroupedAggregate")
    conf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val checkHeaderTrue: Boolean = true

    val invoiceDf = spark.read
      .format("csv")
      .option("header",checkHeaderTrue)
      .option("path","git /order_data-201025-223502.csv")
      .load()

    val summaryDf = invoiceDf.groupBy("Country","InvoiceNo")
      .agg(sum("Quantity").as("TotalQuantity"),
        sum(expr("Quantity * UnitPrice").as("InvoiceValue")))

    summaryDf.show()

    val summaryDf1 = invoiceDf.groupBy("Country","InvoiceNo")
      .agg(expr("sum(Quantity) as  TotalQuantity"),
        expr("sum(Quantity * UnitPrice) as InvoiceValue"))

    summaryDf1.show()

    invoiceDf.createOrReplaceTempView("sales")

    spark.sql(
      """select Country, InvoiceNo , sum(Quantity) as  TotalQuantity,
         sum(Quantity * UnitPrice) as InvoiceValue
         from sales group by Country, InvoiceNo """).show()

    spark.stop()
  }
}
