package chapter2_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object bike_sharing {

  val spark = SparkSession
    .builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()
  /*
  Date,
  RENTED_BIKE_COUNT,
  Hour,
  TEMPERATURE,
  HUMIDITY,
  WIND_SPEED,
  Visibility,
  DEW_POINT_TEMPERATURE,
  SOLAR_RADIATION,
  RAINFALL,
  Snowfall,
  SEASONS,
  HOLIDAY,
  FUNCTIONING_DAY
   */

  val bikeSharingSchema = StructType(Seq(
    StructField("Date", StringType),
    StructField("RENTED_BIKE_COUNT", StringType),
    StructField("Hour", StringType),
    StructField("TEMPERATURE", StringType),
    StructField("HUMIDITY", StringType),
    StructField("WIND_SPEED", StringType),
    StructField("Visibility", StringType),
    StructField("DEW_POINT_TEMPERATURE", StringType),
    StructField("SOLAR_RADIATION", StringType),
    StructField("RAINFALL", StringType),
    StructField("Snowfall", StringType),
    StructField("SEASONS", StringType),
    StructField("HOLIDAY", StringType),
    StructField("FUNCTIONING_DAY", StringType)
  ))


  val opt = Map(
    "inferSchema" -> "false",
    "header" -> "true",
    "path" -> "src/main/resources/bike_sharing.csv",
  )


  val bikeSharingDF = spark.read
    .format("csv")
    .schema(bikeSharingSchema)
    .options(opt)
    .load()

  bikeSharingDF.printSchema()


  import spark.implicits._

  bikeSharingDF.select(
    bikeSharingDF.col("Date"),
    col("Date"),
    column("Date"),
    Symbol("Date"),
    $"Date",
    expr("Date")
  ).show()

  bikeSharingDF.select("Date", "Hour").show()

}
