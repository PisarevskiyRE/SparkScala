package chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object FromJson {

  val spark = SparkSession
    .builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()


  val restaurantSchema = StructType(Seq(
    StructField("average_cost_for_two", LongType),
    StructField("cuisines", StringType),
    StructField("deeplink", StringType),
    StructField("has_online_delivery", IntegerType),
    StructField("is_delivering_now", IntegerType),
    StructField("menu_url", StringType),
    StructField("name", StringType),
    StructField("opened", StringType),
    StructField("photos_url", StringType),
    StructField("url", StringType),
    StructField("user_rating",
      StructType(Seq(
        StructField("aggregate_rating", StringType),
        StructField("rating_color", StringType),
        StructField("rating_text", StringType),
        StructField("votes", StringType)
      )))
  ))


  val restaurantDF = spark.read
    .schema(restaurantSchema)
    .json("src/main/resources/restaurant.json")

  restaurantDF.printSchema
  restaurantDF.show

/*
  val restaurantDF = spark.read
    .format("json")
    .option("inferSchema", "true") //спарк составь нам схему, пожалуйста
    .load("src/main/resources/restaurant.json")
    //.load("src/main/resources/iris.json")
*/

/*
  val restaurantArray: Array[Row] = restaurantDF.take(3) // возвращает итератор по 1

  //restaurantArray.foreach(x => println(x))
  restaurantArray.foreach(println)

*/
}
