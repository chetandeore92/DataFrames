import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions

object  MostAppropriateContact {



  def main(args: Array[String]): Unit = {

    val contactDataList = List(("2022/02/06 10:45:12 pm GMT+5:30", "suraz@gmail.com","Suraj Ghimire","+91-9032412236"),
      ("2022/02/06 10:46:12 pm GMT+5:30", "suraz@gmail.com","Suraj Prasad Ghimire","+91-9032412236"),
    ("2022/02/07 10:45:12 pm GMT+5:30", "suraz@gmail.com","Suraj P Ghimire","+91-9032412236"),
   ("2022/02/07 10:45:12 pm GMT+5:30", "karan@gmail.com","Karan Kumar","+91-7999010203"),
    ("2022/02/08 10:45:12 pm GMT+5:30", "karan@gmail.com","Karan Kumar","+91-7999010203"),
    ("2022/02/09 10:45:12 pm GMT+5:30", "Kiran@gmail.com","Karan Kumar","+91-7999010203"),
    ("2022/02/06 10:45:12 pm GMT+5:30", "kamal@gmail.com","Suraj Ghimire","+91-9032412235"),
    ("2022/02/06 10:46:12 pm GMT+5:30", "kamal@gmail.com","Suraj Ghimire","+91-9032412234"),
    ("2022/03/06 10:45:13 pm GMT+5:30", "kamal@gmail.com","Suraj Ghimire","+91-9032412234"))

    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("MostAppropriateContact")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val contactDataListDf = spark.createDataFrame(contactDataList).toDF("date","email","name","mobile")

    contactDataListDf.show(false)

    val window = Window.partitionBy("email","mobile","name")
      .orderBy(functions.col("date").desc)
      .rowsBetween(Window.unboundedPreceding,Window.currentRow)

  val contactDataWithRowNum =  contactDataListDf.withColumn("row_number",functions.row_number().over(window))
    .where("row_number = 1").drop("row_number")

    contactDataWithRowNum.show(false)

    //def getMostAppropriateName()
    spark.stop()
  }
}
