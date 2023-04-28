package chapter4_2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.Source

object ChipotleStores extends App {

  val spark = SparkSession.builder()
    .appName("RDDs")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  case class Store(
    state: String,
    location: String,
    address: String,
    latitude: Double,
    longitude: Double
  )

//1
  def readStores(filename: String) =
    Source.fromFile(filename)
      .getLines()
      //удаляем первую строчку, тк в ней содержатся названия колонок
      .drop(1)
      // данные в колонках разделяются запятой
      .map(line => line.split(","))
      // построчно считываем данные в case класс
      .map(values => Store(
        values(0),
        values(1),
        values(2),
        values(3).toDouble,
        values(4).toDouble)
      ).toList

  val storesRDD = sc.parallelize(readStores("src/main/resources/chipotle_stores.csv"))

  //storesRDD.foreach(println)

//2
  val storesRDD2 = sc.textFile("src/main/resources/chipotle_stores.csv")
    .map(line => line.split(","))
    .filter(values => values(0) == "Alabama")
    .map(values => Store(
      values(0),
      values(1),
      values(2),
      values(3).toDouble,
      values(4).toDouble))

  //storesRDD2.foreach(println)


  //3
  val storesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/chipotle_stores.csv")

  val storesRDD3 = storesDF.rdd

  //storesRDD3.foreach(println)

  //4
  import spark.implicits._

  val storesDS = storesDF.as[Store]
  val storesRDD4 = storesDS.rdd

  //storesRDD4.foreach(println)


  // distinct
  val locationNamesRDD: RDD[String] = storesRDD.map(_.location).distinct()
  locationNamesRDD.foreach(println)

  //max(len(name))
  implicit val storeOrdering: Ordering[Store] =
    Ordering.fromLessThan[Store]((sa: Store, sb: Store) => sa.location.length < sb.location.length)

  val longestLocationName = storesRDD.max().location
  println(s"location = $longestLocationName") // Birmingham


  //where
  val locationRDD = storesRDD.filter(_.location == longestLocationName)
  locationRDD.foreach(println)

  //group by
  val groupedStoresRDD = storesRDD.groupBy(_.location)
  groupedStoresRDD.foreach(println)

}
