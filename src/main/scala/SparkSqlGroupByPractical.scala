import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession



object SparkSqlGroupByPractical {

  case class BigLog(level: String, date: String)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.master", "local[*]")
    sparkConf.set("spark.app.name", "register Udf")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    /*val bigLogFile = spark.read
      .format("csv")
      .option("header","true")*/

    /*def mapLogs(log: String) ={
      val fields = log.split(",")
      val logger: BigLog= BigLog(fields(0),fields(1))
      logger
    }*/

    /*val myList = List("DEBUG,2015-2-6 16:24:07",
      "WARN,2016-7-26 18:54:43",
      "INFO,2012-10-18 14:35:19",
      "DEBUG,2012-4-26 14:26:50",
      "DEBUG,2013-9-28 20:27:13",
      "INFO,2017-8-20 13:17:27",
      "INFO,2015-4-13 09:28:17",
     "DEBUG,2015-7-17 00:49:27")*/


    //val bigLogFile = spark.createDataFrame(myList).toDF("level", "date")

    //val bigLogRdd = spark.sparkContext.parallelize(myList)

    //import spark.implicits._

    //val bigLogFile = bigLogRdd.map(mapLogs).toDF()

    val bigLogFile = spark.read
      .format("csv")
      .option("header",true)
      .option("path","/Users/chetandeore/Documents/Study/BigData/Week12-Apache Spark - Structured API Part-2/biglog-201105-152517.txt")
      .load()

    bigLogFile.withColumnRenamed("datetime","date")createOrReplaceTempView("biglog")

   /*INT(date_format(date,'M')) => cast(date_format(date,'M') as int)*/

    val monthExtracted = spark.sql(
      """select date_format(date,'MMMM') as month,
        |level,
        |first(INT(date_format(date,'M'))) m,
        |count(1) as count from biglog
        |group by month, level
        |order by m""").drop("m").show(60)

    //monthExtracted.createOrReplaceTempView()

    spark.stop()



  }

}
