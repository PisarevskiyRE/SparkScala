package com.example
package repeat.ch2_1

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object FromJsonWithSchema extends App {

  val spark = SparkSession.builder()
    .appName("FromJson")
    .master("local")
    .getOrCreate()



  val restSchema = StructType(Array(
    StructField("average_cost_for_two", LongType),
    StructField("cuisines", StringType),
    StructField("deeplink", StringType),
    StructField("has_online_delivery", IntegerType),
    StructField("is_delivering_now", IntegerType),
    StructField("menu_url", StringType),
    StructField("name", StringType),
    StructField("opened", StringType),
    StructField("photos_url", StringType),
    StructField("url", StringType),
    StructField("user_rating",
      StructType(Array(
        StructField("aggregate_rating", StringType),
        StructField("rating_color", StringType),
        StructField("rating_text", StringType),
        StructField("votes", StringType)
      ))
  )))





  val restarantDF = spark.read
    .schema(restSchema)
    .json("src/main/resources/restaurant.json")







  restarantDF.show(truncate = false)
  restarantDF.printSchema()
  restarantDF.explain()


}
