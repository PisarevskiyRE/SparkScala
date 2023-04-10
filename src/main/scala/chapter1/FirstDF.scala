package chapter1

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object FirstDF {

  val spark = SparkSession
    .builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()

  val data = Seq(
    Row(
      "s9FH4rDMvds",
      "2020-08-11T22:21:49Z",
      "UCGfBwrCoi9ZJjKiUK8MmJNw",
      "2020-08-12T00:00:00Z"),
    Row(
      "kZxn-0uoqV8",
      "2020-08-11T14:00:21Z",
      "UCGFNp4Pialo9wjT9Bo8wECA",
      "2020-08-12T00:00:00Z"),
    Row(
      "QHpU9xLX3nU",
      "2020-08-10T16:32:12Z",
      "UCAuvouPCYSOufWtv8qbe6wA",
      "2020-08-12T00:00:00Z")
  )

  // оказывается не подразумевается приведение к времени, будет потом
  val schema = Array(
    StructField("videoId", StringType, false),
    StructField("publishedAt", StringType, false),
    StructField("channelId", StringType, false),
    StructField("trendingDate", StringType, false),
  )

  val sparkContex = spark.sparkContext

  val df = spark.createDataFrame(
    sparkContex.parallelize(data),
    StructType(schema)
  )

  //df.printSchema() // принтим схему
  df.show
}
