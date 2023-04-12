package chapter2_2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object FromFile {

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

/*

  через монады

  val restaurnatDF = spark.read
    .format("json")
    .schema(restaurantSchema) // либо .option("inferSchema", "true")
    .option("mode", "failFast") // варианты: failFast, dropMalformed, permissive (default)
    .option("path", "src/main/resources/restaurant.json")
    .load
*/
  // через кортеж Map(ключ -> свойство)
  // p.s. можно выделить настройки в отдельную переменную

  val opt = Map(
    "inferSchema" -> "true",
    "mode" -> "failFast",
    "path" -> "src/main/resources/restaurant.json"
  )

  val restaurnatDF = spark.read
    .format("json")
    .options(opt)
    .load()

/*
  mode - параметр
  , который указывает
  , что делать с поврежденными записями:

  permissive - (дефолтный, если не указать параметр mode, то именно permissive будет использован.Это означает, что вместо поврежденных записей будет использован null)
  failFast - (при обнаружении поврежденной записи выбрасывается исключение)
  dropMalformed - (поврежденные записи просто игнорируются)

*/



}
