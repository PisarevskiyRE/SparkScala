package chapter2_3

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object selectTopBikeCSV2 {

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

  // не понимаю как работать правильно со схемой...
  // мы должны на интуитивном уровне примерно прикидывать что есть в колонках
  // или дальше будет какой-то человеческий иструмент который сможет нормально проанализировать допустим выборкой 1% данных каждые 20%,
  // ну то есть теорией вероятности рандомно что то чекнуть если данных петабайты
  // угадывать же схему и типы если мы её не создавали и не заполняем не очень хорошо?
  // возможно вперед паровоза бегу и дальше все будет расказано, извините на всякий случай
  //

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

  //bikeSharingDF.printSchema()

  println("count ->" + bikeSharingDF.select("*").count.toString)
  println("distinct ->" + bikeSharingDF.select("HOLIDAY").distinct.toString)

  val bikesWithColumnRenamed = bikeSharingDF.withColumnRenamed("TEMPERATURE", "Temp")
  bikesWithColumnRenamed.show(3)

  bikeSharingDF.drop("Date", "Hour").show()


  bikeSharingDF
    .withColumn(
      "is_holiday",
      when(col("HOLIDAY") === "No Holiday", false)
        .when(col("HOLIDAY") === "Holiday", true)
        .otherwise(null)
    ).show


  bikeSharingDF
    .groupBy("Date")
    .agg(
      functions.sum("RENTED_BIKE_COUNT").as("bikes_total"))
    .orderBy("Date")
    .show(3)


  println("count222 ->" +

  bikeSharingDF
  .select("Date")
    .where("RENTED_BIKE_COUNT = 254")
    .where("TEMPERATURE > 0")
    .distinct
    .count.toString
  )






}
