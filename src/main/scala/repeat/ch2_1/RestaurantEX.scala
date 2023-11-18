package com.example
package repeat.ch2_1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object RestaurantEX extends App {

  val spark = SparkSession.builder()
    .appName("RestaranEx")
    .master("local")
    .getOrCreate()

  val restSchema = StructType(Seq(
    StructField("average_cost_for_two",LongType,true),
    StructField("cuisines",StringType,true),
    StructField("deeplink",StringType,true),
    StructField("has_online_delivery",LongType,true),
    StructField("is_delivering_now",LongType,true),
    StructField("menu_url",StringType,true),
    StructField("name",StringType,true),
    StructField("url",StringType,true),
    StructField("user_rating", StructType(Seq(
      StructField("aggregate_rating",StringType,true),
      StructField("rating_color",StringType,true),
      StructField("rating_text",StringType,true),
      StructField("votes",StringType,true)
    )),true)
  ))


  val restDf = spark.read
    .schema(restSchema)
    .json("src/main/resources/restaurant_ex.json")

  restDf.show()
  restDf.printSchema()

}
