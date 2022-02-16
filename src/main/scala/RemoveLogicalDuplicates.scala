import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object RemoveLogicalDuplicates {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.master","local[*]")

    val spark = SparkSession.builder()
      val spark1 = spark.config(sparkConf)
        val spark2 = spark1.getOrCreate()

    val file1 = spark2.sparkContext.textFile("/Users/chetandeore/Documents/Study/testData.csv")



    val file = spark2.read.format("csv")
      .option("path","/Users/chetandeore/Documents/Study/testData.csv")
      .load().toDF("from","to")




    file.collect().foreach(println)

    file.createOrReplaceTempView("friendRequest")

    spark2.sql("select  distinct least(from,to) from , greatest(from,to) to from friendRequest").show()

    spark2.stop()
  }

}
