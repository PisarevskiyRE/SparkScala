package chapter2_2

import org.apache.spark.sql.SparkSession

object FromFileCSV2 {

  val spark = SparkSession
    .builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()


  val opt = Map(
    "inferSchema" -> "true",
    "header" -> "true",
    "sep" -> ",",
    "nullValue" -> "n/a",
    "mode" -> "dropMalformed"
  )

  val topspotifyDF = spark.read
    .options(opt)
    .csv("src/main/resources/subtitles.csv")

  topspotifyDF.show
}


/*

 1 - + - - -
 2 + + + + +
 3 + + + + +
 4 + + + + +
 5 + - + + -


 */
