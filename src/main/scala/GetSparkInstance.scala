import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object GetSparkInstance {

  def get(sparkConf: SparkConf) : SparkSession = {
    SparkSession.builder().config(sparkConf).getOrCreate()
  }

}
