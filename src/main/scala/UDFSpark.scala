import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr


object UDFSpark {

  def checkAge(age : Int) = {
    if(age > 18) "Y" else "N"
  }

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()

    sparkConf.set("spark.master","local[*]")
    sparkConf.set("spark.app.name","register Udf")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()




    val df1 = spark.read.format("csv")
      .option("path","/Users/chetandeore/Documents/Study/BigData/Week12-Apache Spark - Structured API Part-2/ageDataset.csv")
      .option("inferSchema",true)
      .load().toDF("name","age","city")

    //this is must, so that driver will serialize this func and will send it to all executors
    //column object expression UDF
    /*
        val parseAge = udf(checkAge(_:Int):String)
    */

    //column object expression UDF
    /*
        val df2 = df1.withColumn("adult",parseAge(col("age")))
    */



    // SQL/String expression UDF -> Registering UDF

    // spark.udf.register("parseAge",checkAge(_:Int):String)


    spark.udf.register("parseAge",(x:Int)=>{if(x > 18) "Y" else "N"})

    //calling UDF

    val df2 = df1.withColumn("adult",expr("parseAge(age)"))

    df2.show()

    df1.createOrReplaceTempView("people")

    //As we use sql/string expression UDF this fun will be registered in catalog as well
    spark.catalog.listFunctions().filter(x => x.name == "parseAge" ).show(false)

    //as the function is registered in Spark catalog we can call it as a SQL fun
    spark.sql("select name,parseAge(age) as adult from people").show



    spark.stop()

  }

}