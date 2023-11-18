package com.example
package repeat.ch3_2
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}

object Shoes extends App{

  val spark = SparkSession.builder()
    .appName("Shoes")
    .master("local")
    .getOrCreate()


  val df = spark.read
    .option("inferSchema","true")
    .option("header","true")
    .csv("src/main/resources/athletic_shoes.csv")

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




  def dropNa(cols: Seq[String])(df: DataFrame): DataFrame = {
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

  import spark.implicits._

  val transforms = df
    .transform(dropNa(Seq("item_name", "item_category")))
    .transform(coalesceValues(List(col("item_after_discount"), col("item_price"))))
    .transform(replaceNull(
      Map("item_rating" -> 0,
        "buyer_gender"-> "unknown")))
    .transform(replaceAllNull("n/a"))
    .as[Shoes]




  transforms.show(100, truncate = false)

}
