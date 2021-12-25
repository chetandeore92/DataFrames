import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object JsonRead {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()

    sparkConf.set("spark.app.name","JsonRead")
    sparkConf.set("spark.master","local[2]")

    val spark = SparkSession.builder()
      .config(sparkConf).getOrCreate()


    //headers are not available in case of json
    //inferSchema is by default true in case of format = json
    val jsonFile = spark.read
      .format("json")
      .option("path","/Users/chetandeore/Documents/Study/BigData/Week11-Apache Spark - Structured API Part-1/players-201019-002101.json")
      .option("mode","dropmalformed")
      .load

    jsonFile.show()

    jsonFile.printSchema()



    scala.io.StdIn.readLine()


  }
}
