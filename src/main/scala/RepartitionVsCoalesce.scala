import org.apache.spark.SparkConf

object RepartitionVsCoalesce {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RepartitionVsCoalesce")
    val spark = SparkInstance.get(sparkConf)

    val rdd1 = spark.sparkContext.textFile("/Users/chetandeore/Documents/Study/BigData/Week12-Apache Spark - Structured API Part-2/biglog-201105-152517.txt")

    //Current Partition 2
    println("Current Partition "+rdd1.getNumPartitions)

    //Below transformation needs 100% shuffling (Partitions will be of equal size)

    //val rdd2 = rdd1.repartition(1)


    //No shuffling (or minimal shuffling) (Partitions will be of unequal size)

    val rdd2 = rdd1.coalesce(1)

    rdd2.collect()

    println("Later Partition "+rdd2.getNumPartitions)


   // scala.io.StdIn.readLine()

    spark.stop()

  }

}
