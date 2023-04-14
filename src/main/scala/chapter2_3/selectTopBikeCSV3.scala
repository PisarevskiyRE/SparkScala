package chapter2_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object selectTopBikeCSV3 {

  val spark = SparkSession
    .builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()

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



  bikeSharingDF
    .withColumn(
      "is_workday",
        when(col("HOLIDAY") === "Holiday" && col("FUNCTIONING_DAY") === "No", 0)
        .otherwise(1)
    )
    .select("HOLIDAY", "FUNCTIONING_DAY", "is_workday").distinct.show

/*

результат
+----------+---------------+----------+
|   HOLIDAY|FUNCTIONING_DAY|is_workday|
+----------+---------------+----------+
|   Holiday|             No|         0|
|   Holiday|            Yes|         1|
|No Holiday|             No|         1|
|No Holiday|            Yes|         1|
+----------+---------------+----------+

 */
}
