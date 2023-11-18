package com.example
package repeat.ch2_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object Bike extends App {

  val spark = SparkSession.builder()
    .appName("Bike")
    .master("local")
    .getOrCreate()


  val bikeDf = spark.read
    .format("csv")
    .options(Map(
      "inferSchema" -> "false",
      "header" -> "true",
      "sep" -> ",",
      "mode" -> "permissive", //ошибки в null
      "path" -> "src/main/resources/bike_sharing.csv",
      "nullValue" -> "n/a"
    ))
    .load()


  import spark.implicits._

  val bikeSharingDF = bikeDf.withColumn("is_workday",

    when(col("HOLIDAY") === "Holiday" && col("FUNCTIONING_DAY") === "No", 0)
      .otherwise(1)
  )
    .select(
      col("HOLIDAY"),
      col("FUNCTIONING_DAY"),
      col("is_workday"),
    )
    .distinct()

  bikeSharingDF.show()



  val tempDf = bikeDf
    .select(
      col("Date"),
      col("TEMPERATURE").cast(DoubleType)
    )
    .groupBy("Date")
    .agg(
      max("TEMPERATURE").as("max"),
      min("TEMPERATURE").as("min")
    )


  tempDf.show()

}
