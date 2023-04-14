package chapter2_3

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object selectTopBikeCSV4 {

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

// можно было конечно вывернуться и оконными наверное как то достать :)
  //  bikeSharingDF
//    .select("Date", "TEMPERATURE")
//    .withColumn(
//      "min_temp",
//      functions.max("TEMPERATURE").over(Window.partitionBy("Date"))
//    )
//    .withColumn(
//      "max_temp",
//      functions.max("TEMPERATURE").over(Window.partitionBy( "Date"))
//    )
//    .groupBy(col("Date"))

  bikeSharingDF
    .groupBy("Date")
    .agg(
      min("TEMPERATURE").as("min_temp"),
      max("TEMPERATURE").as("max_temp")
    )
    .orderBy("Date")
    .show



  /* результат
+----------+--------+--------+
  |      Date|min_temp|max_temp|
  +----------+--------+--------+
  |01/01/2018|    -0.1|     3.7|
  |01/02/2018|      -1|     1.1|
  |01/03/2018|    -0.8|       3|
  |01/04/2018|    11.7|    18.2|
  |01/05/2018|    17.8|    23.1|
  |01/06/2018|    17.5|    29.9|
  |01/07/2018|    21.1|    22.7|
  |01/08/2018|    27.8|    39.4|
  |01/09/2018|    20.9|    30.5|
  |01/10/2018|    13.1|    19.3|
  |01/11/2018|    10.5|     9.9|
  |01/12/2017|    -0.3|       3|
  |02/01/2018|    -0.4|     1.7|
  |02/02/2018|    -0.5|     3.6|
  |02/03/2018|    -1.8|     5.2|
  |02/04/2018|    14.9|    23.7|
  |02/05/2018|    10.4|     9.5|
  |02/06/2018|    17.7|    29.8|
  |02/07/2018|    20.7|    25.3|
  |02/08/2018|    30.4|    37.9|
  +----------+--------+--------+
  only showing top 20 rows

   */


}
