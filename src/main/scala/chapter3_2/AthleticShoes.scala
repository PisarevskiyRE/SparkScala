package chapter3_2

import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object AthleticShoes extends App {

  val spark = SparkSession
    .builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()


  val ShoesDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/athletic_shoes.csv")


  import spark.implicits._

  case class Shoes(
                    item_category: String,
                    item_name: String,
                    item_after_discount: String,
                    item_price: String,
                    percentage_solds: Int,
                    item_rating: Int,
                    item_shipping: String,
                    buyer_gender: String
                  )

  def dropWithoutValueCols(cols: List[String])(df: DataFrame): DataFrame = {
    df.na.drop(cols)
  }

  def coalesceValues(cols: List[Column])(df: DataFrame): DataFrame = {
    df.withColumn(
      cols(0).toString(),
      coalesce(cols: _*)
    )
  }

  def replaceNull(rules: Map[String, Any])(df: DataFrame): DataFrame = {
    df.na.fill(rules)
  }

  def replaceAllNull(dafaultValue: String)(df: DataFrame): DataFrame = {
    df.na.fill(dafaultValue)
  }

  val ShoesFixedDF = ShoesDF
    .transform(dropWithoutValueCols(List("item_name", "item_category")))
    .transform(coalesceValues(List($"item_after_discount", $"item_price")))
    .transform(replaceNull(
      Map("item_rating" -> 0,
          "buyer_gender"-> "unknown")))
    .transform(replaceAllNull("n/a"))


  val ShoesDS = ShoesFixedDF.as[Shoes]

  ShoesDS.show
  /*
  результат
  +--------------+--------------------+-------------------+----------+----------------+-----------+--------------------+------------+
  | item_category|           item_name|item_after_discount|item_price|percentage_solds|item_rating|       item_shipping|buyer_gender|
  +--------------+--------------------+-------------------+----------+----------------+-----------+--------------------+------------+
  |Athletic Shoes|Nike SB Check Sol...|          299.00SAR| 211.48SAR|              94|        100|       FREE Shipping|         men|
  |Athletic Shoes|Nike Md Runner 2 ...|          349.00SAR| 289.99SAR|              80|         88|       FREE Shipping|         men|
  |Athletic Shoes|Nike air Heights ...|          419.00SAR| 337.48SAR|              77|          0|       FREE Shipping|       women|
  |Athletic Shoes|Nike Fitness Shoe...|          319.00SAR| 211.48SAR|              76|         80|       FREE Shipping|         men|
  |Athletic Shoes|adidas ASWEERUN S...|          259.00SAR| 194.99SAR|              94|          0|Eligible for FREE...|         men|
  |Athletic Shoes|Nike Sport Sneake...|          319.00SAR| 234.99SAR|              30|          0|       FREE Shipping|         men|
  |Athletic Shoes|Adidas Duramo 9 R...|          319.00SAR| 233.98SAR|              68|        100|       FREE Shipping|         men|
  |Athletic Shoes|Nike Fitness Shoe...|          389.00SAR| 280.98SAR|              71|          0|       FREE Shipping|         men|
  |Athletic Shoes|Nike Air Zoom Peg...|          609.00SAR| 470.99SAR|              60|          0|       FREE Shipping|         men|
  |Athletic Shoes|Nike Court Lite 2...|          339.00SAR| 236.99SAR|              33|          0|       FREE Shipping|         men|
  |Athletic Shoes|Adidas VS Pace Nu...|          259.00SAR| 186.98SAR|              86|         90|Eligible for FREE...|         men|
  |Athletic Shoes|Nike Fitness Shoe...|          319.00SAR| 211.48SAR|              87|        100|       FREE Shipping|         men|
  |Athletic Shoes|Adidas Grand Cour...|          249.00SAR| 177.99SAR|              75|        100|Eligible for FREE...|         men|
  |Athletic Shoes|adidas ASWEERUN S...|          259.00SAR| 194.99SAR|              40|          0|Eligible for FREE...|         men|
  |Athletic Shoes|Skechers GO WALK ...|          279.00SAR| 175.98SAR|              68|         90|Eligible for FREE...|         men|
  |Athletic Shoes|Nike air Monarch ...|          279.00SAR| 234.99SAR|              60|         90|       FREE Shipping|         men|
  |Athletic Shoes|Nike Explore Stra...|          339.00SAR| 245.98SAR|              57|          0|       FREE Shipping|       women|
  |Athletic Shoes|Nike Tanjun Sneak...|          349.00SAR| 284.49SAR|              61|         88|       FREE Shipping|         men|
  |Athletic Shoes|Nike Quest 2 Nylo...|          280.98SAR| 280.98SAR|              71|          0|       FREE Shipping|         men|
  |Athletic Shoes|Puma Ella Sneaker...|          249.00SAR| 191.49SAR|              66|          0|Eligible for FREE...|       women|
  +--------------+--------------------+-------------------+----------+----------------+-----------+--------------------+------------+

   */
}
