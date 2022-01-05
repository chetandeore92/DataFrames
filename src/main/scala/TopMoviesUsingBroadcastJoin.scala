import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

case class Movie(id: Int, name: String)

case class MovieRating(userId: Int, movieId: Int, rating: Float, time: Int )

object TopMoviesUsingBroadcastJoin {

  def main(args: Array[String]): Unit = {



    def movieSchema(line: String) = {
      val fields = line.split("::")
      Movie(fields(0).toInt,fields(1))
    }

    def movieRatingSchema(line : String) = {
      val fields = line.split("::")
      MovieRating(fields(0).toInt,fields(1).toInt,fields(2).toFloat,fields(3).toInt)
    }


    val sparkConf = new SparkConf()

    sparkConf.set("spark.master","local[*]")
    sparkConf.set("spark.app.name","TopMoviesUsingBroadcastJoin")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val movieRatingFile = spark.read
      .format("csv")
      .option("delimiter","::")
      .option("path","/Users/chetandeore/Documents/Study/BigData/Week11-Apache Spark - Structured API Part-1/ratings-201019-002101.dat")
      .load()
      .toDF("userId","movieId","rating","time")

    movieRatingFile.show(false)



    /*val movieRatingFileRdd = spark.sparkContext
      .textFile("/Users/chetandeore/Documents/Study/BigData/Week11-Apache Spark - Structured API Part-1/ratings-201019-002101.dat")
*/

    import spark.implicits._
   // val movieRatingFile = movieRatingFileRdd.map(movieRatingSchema).toDF()

   // movieRatingFile.show(false)

    /*val moviesFile = spark.read
      .format("csv")
      .option("path","/Users/chetandeore/Documents/Study/BigData/Week11-Apache Spark - Structured API Part-1/movies-201019-002101.dat")
      .option("header",false)
      .load()

moviesFile.show(false)*/

    val moviesFileRdd = spark.sparkContext
      .textFile("/Users/chetandeore/Documents/Study/BigData/Week11-Apache Spark - Structured API Part-1/movies-201019-002101.dat")


    val moviesFile = moviesFileRdd.map(movieSchema).toDS()
    moviesFile.show(false)

    val joinCondition = movieRatingFile.col("movieId") === moviesFile.col("id")
    val joinType = "inner"

    val movieJoinedDf = movieRatingFile.join(broadcast(moviesFile),joinCondition,joinType)

    movieJoinedDf.createOrReplaceTempView("movie")

    spark.sql(
      """select name, avg(rating) AvgRating, count(*) ratingCount
        from movie
        group by name
        having AvgRating > 4.5 and ratingCount > 500
        order by AvgRating""").show(100,false)

    spark.stop()

  }


}
