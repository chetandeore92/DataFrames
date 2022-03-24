import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkInstance {

  def get(sparkConf: SparkConf) : SparkSession = {
    SparkSession.builder().config(sparkConf).getOrCreate()
  }

}
