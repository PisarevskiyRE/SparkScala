package com.example
package repeat.ch2_1

import org.apache.spark.sql.{Row, SparkSession}

object FromJson extends App {

  val spark = SparkSession.builder()
    .appName("FromJson")
    .master("local")
    .getOrCreate()


  val restarantDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/restaurant.json")


  val takedOne: Array[Row] = restarantDF.take(1)

  takedOne.foreach(println)



  restarantDF.show(truncate = false)
  restarantDF.printSchema()
  restarantDF.explain()


}
