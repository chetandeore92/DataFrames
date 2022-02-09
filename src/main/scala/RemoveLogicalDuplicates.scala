import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RemoveLogicalDuplicates {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.master","local[*]")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val file = spark.read.format("csv")
      .option("path","/Users/chetandeore/Documents/Study/testData.csv")
      .load().toDF("from","to")

    file.collect().foreach(println)

    file.createOrReplaceTempView("friendRequest")

    spark.sql("select distinct least(from,to) from, greatest(from,to) to from friendRequest").show()

    spark.stop()
  }

}
