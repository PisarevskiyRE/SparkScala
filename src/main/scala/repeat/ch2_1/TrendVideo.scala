package com.example
package repeat.ch2_1

import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, SparkSession}

object TrendVideo extends App {


  val spark = SparkSession.builder()
    .appName("TrendVideo")
    .master("local")
    .getOrCreate()


  val data = Seq(
    Row("s9FH4rDMvds", "2020-08-11T22:21:49Z", "UCGfBwrCoi9ZJjKiUK8MmJNw", "2020-08-12T00:00:00Z"),
    Row("kZxn-0uoqV8", "2020-08-11T14:00:21Z", "UCGFNp4Pialo9wjT9Bo8wECA", "2020-08-12T00:00:00Z"),
    Row("QHpU9xLX3nU", "2020-08-10T16:32:12Z", "UCAuvouPCYSOufWtv8qbe6wA", "2020-08-12T00:00:00Z")
  )

  val schema = StructType(Array(
    StructField("videoId", StringType, false),
    StructField("publishedAt", StringType, false),
    StructField("channelId", StringType, false),
    StructField("trendingDate", StringType, false),
  ))


  val videoDf = spark.createDataFrame(
    spark.sparkContext.parallelize(data),
    schema
  )

  videoDf.show(truncate = false)
  videoDf.printSchema()
}
