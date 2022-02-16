import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}


case class order(order_id: Int,order_date: String,order_customer_id: Int ,order_status : String)

object SparkSessionFactory
{
  def getSparkSession: SparkSession = {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.app.name", "testDsAndDf")
    sparkConf.set("spark.master","local[*]")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    spark
  }
}

object TestClass {

  def getTheSchema(spark: SparkSession) = {

    getTheFile(spark).printSchema()
  }





  def getTheFile(spark: SparkSession): Dataset[Row] ={
    val inputFile = spark.read
      .format("csv")
     // .option("path", "/Users/chetandeore/Documents/Study/BigData/Week12-Apache\\ Spark\\ -\\ Structured\\ API\\ Part-2/orders-201025-223502.csv")
    .option("path", "/Users/chetandeore/Documents/Study/BigData/Week12-Apache\\ Spark\\ -\\ Structured\\ API\\ Part-2/ds.csv")
      //.schema(orderSchema)
      //.schema("order_id String,order_date Int,order_customer_id String,order_status String")
      .option("inferSchema", true)
       .option("header", true)
      .option("mode","DROPMALFORMED")
      .load()

   /* val inputFile = spark.sparkContext.textFile("/Users/chetandeore/Documents/Study/BigData/Week12-Apache Spark - Structured API Part-2/orders-201025-223502.csv")
      .map(parser).toDS()*/

    inputFile.printSchema()

   val file = inputFile.na.drop("any")

    file
  }

  /*def parser(line : String): order = {
   val arr = line.split(",")

    order(arr(0),arr(1),arr(2),arr(3))

  }*/

  def getTheFileCount(spark: SparkSession) = {

   /* val orderSchema = StructType(List(
      StructField("order_id",IntegerType,nullable = true),
      StructField("order_date",StringType),
      StructField("order_customer_id",IntegerType,false),
      StructField("order_status",StringType,false)
    ))*/

    val inputFile = getTheFile(spark)
  // inputFile.show()
   val count = inputFile

    count

  }
}

  object TestClassMainMethod {


    def main(args: Array[String]): Unit = {


   val spark = SparkSessionFactory.getSparkSession

     // val count = TestClass.getTheFileCount(spark)

    //  val schema = TestClass.getTheSchema(spark)

     /* TestClass.getTheFile(spark).write
        .format("csv")
        //.option("path","/Users/chetandeore/Desktop/new")
       .partitionBy("order_status")
        .bucketBy(numBuckets = 4,colName = "order_customer_id")
        .mode(SaveMode.Overwrite)
        .saveAsTable("order")*/

     /* println("Databases: "+spark.catalog.listDatabases().show(false))
      println("Tables: "+spark.catalog.listTables("default").show(false))*/

     // scala.io.StdIn.readLine()._

      import spark.implicits._
     val inputFile = TestClass.getTheFile(spark).toDF("order_id" ,"order_date" ,"order_customer_id" ,"order_status").as[order]

      inputFile.show(false)



     val window1 = Window.partitionBy(colName = "order_status")
        .orderBy("order_date")
        .rowsBetween(Window.unboundedPreceding,Window.currentRow)

  val finalData = inputFile.withColumn("running_total",functions.count("order_id").over(window1))
        .createOrReplaceTempView("order")

      spark.sql("select order_status, order_date,running_total from order").collect().foreach(println)

      spark.stop()



    }
  }