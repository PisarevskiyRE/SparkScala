// переименовал пакеты
package chapter2_1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

// первый для тестов примеров
object FromJson2 {

  val spark = SparkSession
    .builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()

  // а можно как то сделать так что бы спарк черновой вариант
  // схемы предлага в scala формате, а мы уже потом уточняли детали схемы?
  val restaurantExSchema = StructType(Seq(
    StructField("average_cost_for_two", LongType),
    StructField("cuisines", StringType),
    StructField("deeplink", StringType),
    StructField("has_online_delivery", IntegerType),
    StructField("is_delivering_now", IntegerType),
    StructField("menu_url", StringType),
    StructField("name", StringType),
    StructField("url", StringType),
    StructField("user_rating",
      StructType(Seq(
        StructField("aggregate_rating", StringType),
        StructField("rating_color", StringType),
        StructField("rating_text", StringType),
        StructField("votes", StringType)
      )))
  ))

  // не уверен нужно ли тут спарк контекст выделать?
  // и не особо понимаю когда это нужно(есть подозрения что это уже будет влять на физическое выполнение,
  // параллелизм кластеры и всякие такие умные штуки :))
  val restaurantExDF = spark.read
    .schema(restaurantExSchema)
    .json("src/main/resources/restaurant_ex.json")

  //restaurantExDF.printSchema
  restaurantExDF.show
}
