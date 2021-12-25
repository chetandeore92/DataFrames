import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, unix_timestamp}
import org.apache.spark.sql.types.DateType

object ListToDF {

  def main(args: Array[String]): Unit = {
    val myList = List((1,"2013-07-25 00:00:00.0",11599,"CLOSED"),
    (2,"2013-07-25 00:00:00.",256,"PENDING_PAYMENT"),
    (3,"2013-07-25 00:00:00.0",12111,"COMPLETE"),
    (4,"2013-07-25 00:00:00.0",8827,"CLOSED"),
    (5,"2013-07-25 00:00:00.0",8827,"COMPLETE"),
    (6,"2013-07-25 00:00:00.0",7130,"COMPLETE"),
    (9,"2013-07-25 00:00:00.0",5657,"PENDING_PAYMENT"))

    val sparkConf = new SparkConf()

    sparkConf.set("spark.master","local[*]")
    sparkConf.set("spark.app.name","register Udf")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()


    val df1 = spark.createDataFrame(myList).toDF("orderid","orderdate","order_customer_id","order_status")

    val df2 = df1
      .withColumn("orderdate",unix_timestamp(col("orderdate").cast(DateType)))
      .withColumn("newid",monotonically_increasing_id())
      .dropDuplicates("order_customer_id","orderdate")
      .drop("orderid")
      .sort("orderdate")



    df2.show();

    spark.stop()

  }
}
