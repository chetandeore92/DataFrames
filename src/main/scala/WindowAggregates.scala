import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object WindowAggregates{
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
      .option("path","/Users/chetandeore/Documents/Study/BigData/Week12-Apache Spark - Structured API Part-2/windowdata-201025-223502.csv")
      .load().toDF("country","weeknum","numInvoices","totalquantity","invoicevalue")

    /*val myWindow = Window.partitionBy("country")
      .orderBy("weeknum")
      .rowsBetween(Window.unboundedPreceding,Window.currentRow)

    val myWindow1 = Window.partitionBy("country")
      .orderBy("weeknum")
      .rowsBetween(start = Window.unboundedPreceding, end =Window.currentRow)



    invoiceDf.withColumn("runningTotal",functions.sum("invoicevalue").over(myWindow)).show()

    invoiceDf.withColumn("runningTotal",functions.sum("invoicevalue").over(myWindow1)).show()
*/

    spark.stop()

  }
}
