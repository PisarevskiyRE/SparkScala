package chapter2_2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object FromFileCSV {

  val spark = SparkSession
    .builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()


  val opt = Map(
    "inferSchema" -> "true",
    "header" -> "true",
    "sep" -> ",",
    "nullValue" -> "n/a"
  )

  val topspotifyDF = spark.read
    .options(opt)
    .csv("src/main/resources/topspotify.csv")

  topspotifyDF.show
}
