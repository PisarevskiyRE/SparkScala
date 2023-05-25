package chapter4_3

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import io.circe.parser._
import io.circe.generic.auto._

import scala.io.Source

object AmazonProducts extends App{
  val spark = SparkSession.builder()
    .appName("RDDs")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  case class Product(
                      uniq_id: Option[String],
                      product_name: Option[String],
                      manufacturer: Option[String],
                      price: Option[String],
                      number_available: Option[String],
                      number_of_reviews: Option[Int]
                    )

  def readJson(filename: String) =
    Source
      .fromFile(filename)
      .getLines()
      .toList

  val jsonRDD: RDD[String] = sc.parallelize(readJson("src/main/resources/amazon_products.json"))

  val productsRDD: RDD[Product] = jsonRDD.flatMap { json =>
    decode[Product](json).toOption
  }

  productsRDD.foreach(println)
/**
 * Product(Some(nd57efa5dbd3d667f26eb3d3ab504464),Some(Hornby 3456),None,Some(£73.42),Some(1 new),None)
 * Product(Some(dfc1efa5dbd3d667f26eb3d3ab504464),Some(Hornby R3246TTS LNER 2-8-2 'Cock O' The North' P2 Class with TTS Sound),None,Some(£3.42),Some(1 new),Some(0))
 * Product(Some(mbdf5efa5dbd3d667f26eb3d3ab504464),None,Some(Hornby),Some(£3.42),Some(5 new),Some(2))
 * Product(Some(eac7efa5dbd3d667f26eb3d3ab504464),Some(Hornby 2014 Catalogue),Some(Hornby),Some(£3.42),Some(5 new),Some(2))
 * Product(Some(fac7efa5dbd3d667f26eb3d3ab504464),Some(Plarail - AS-07 Shinkansen Series 700 (Model Train)),Some(Takara Tomy),Some(£13.42),Some(35 new),Some(1))
 * Product(Some(wec2efa5dbd3d667f26eb3d3ab504464),Some(Hornby 2014 Catalogue),Some(Hornby),None,Some(7 old),Some(3))
 * Product(Some(eac7efa5dbdbjghjs823os7wbcad504464),Some(Faller 140431),Some(Faller),Some(£73.67),Some(7 old),Some(4))
 * ...
 */
}
