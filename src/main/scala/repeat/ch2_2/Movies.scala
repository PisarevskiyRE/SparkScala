package com.example
package repeat.ch2_2

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object Movies extends App {

  val spark = SparkSession.builder()
    .appName("Movies")
    .master("local")
    .getOrCreate()

  val schema = StructType(Seq(
    StructField("_c0", StringType),
    StructField("show_id", StringType),
    StructField("type", StringType),
    StructField("title", StringType),
    StructField("director", StringType),
    StructField("cast", StringType),
    StructField("country", StringType),
    StructField("date_added", StringType),
    StructField("release_year", StringType),
    StructField("rating", StringType),
    StructField("duration", StringType),
    StructField("listed_in", StringType),
    StructField("description", StringType),
    StructField("year_added", StringType),
    StructField("month_added", StringType),
    StructField("season_count", StringType),
  ))


  val moviesDf = spark.read
    .format("csv")
    .schema(schema)
    .options(Map(
      "inferSchema" -> "false",
      "header" -> "true",
      "sep" -> ",",
      "mode" -> "permissive",
      "path" -> "src/main/resources/movies_on_netflix.csv",
      "nullValue" -> "n/a"
    ))
    .load()


  moviesDf.show(100, truncate = false)

  moviesDf.printSchema()



  moviesDf.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/movies.parquet")

}
