package chapter2_2

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object ToFileCSV {

  val spark = SparkSession
    .builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()



  val moviesSchema = StructType(Seq(
    StructField("id", StringType), // оказывается в схеме есть столбец без названия
    StructField("show_id", StringType),
    StructField("showType", StringType),
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
    StructField("season_count", IntegerType),
  ))


  // p.s. можно выделить настройки в отдельную иммутабельную переменную
  val opt = Map(
    "inferSchema" -> "false",
    "header" -> "true",
    //"sep" -> ",",
    //запятая по умолчанию для csv
    //"mode" -> "failFast",
    "path" -> "src/main/resources/movies_on_netflix.csv",
    //"nullValue" -> "n/a"
  )

  val moviesDF = spark.read
    .format("csv")
    .schema(moviesSchema)
    .options(opt)
    .load()

  moviesDF.printSchema()

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data")



}
