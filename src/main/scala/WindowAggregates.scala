import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window

object WindowAggregates{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    conf.set("spark.app.name", "GroupedAggregate")
    conf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val checkHeaderTrue: Boolean = true

    /*val invoiceDf = spark.read
      .format("csv")
      .option("header",checkHeaderTrue)
      .option("path","/Users/chetandeore/Documents/Study/BigData/Week12-Apache Spark - Structured API Part-2/windowdata-201025-223502.csv")
      .load().toDF("country","weeknum","numInvoices","totalquantity","invoicevalue")*/

    /*val myWindow = Window.partitionBy("country")
      .orderBy("weeknum")
      .rowsBetween(Window.unboundedPreceding,Window.currentRow)

    val myWindow1 = Window.partitionBy("country")
      .orderBy("weeknum")
      .rowsBetween(start = Window.unboundedPreceding, end =Window.currentRow)



    invoiceDf.withColumn("runningTotal",functions.sum("invoicevalue").over(myWindow)).show()

    invoiceDf.withColumn("runningTotal",functions.sum("invoicevalue").over(myWindow1)).show()
*/

    val simpleData = Seq(("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Robert", "Sales", 4200),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Maria", "Finance", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100),
      ("Saif", "Marketing", 4100)
    )

    import spark.implicits._
    val df = simpleData.toDF("employee_name", "department", "salary")//.select("employee_name","salary")
    df.show()

    val window = Window
      .partitionBy("employee_name","department")
      .orderBy("salary")
      .rowsBetween(Window.unboundedPreceding,Window.currentRow)

    val dfWithRowNum = df.withColumn("row_num",functions.row_number().over(window))

    dfWithRowNum.createOrReplaceTempView("employee")

    spark.sql("select employee_name, department, salary from employee where row_num = 1 ").show()

    spark.stop()

  }
}
