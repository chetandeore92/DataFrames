import org.apache.spark.sql.SparkSession

class ScalaTest  {


  @org.junit.Test
  def test1 = {

    val expected = 20
    var actual = TestClass.function(4, 5)((x, y) => x * y)
    assert(expected == actual)
  }

  @org.junit.Test
  def test2 = {
    val expected = 68881

    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()


    val actual = TestClass.getTheFileCount(spark)

    assert(actual == expected)
    spark.stop()

  }
}

