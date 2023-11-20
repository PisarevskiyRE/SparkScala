package com.example
package repeat.ch4_2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.Source


object Store extends App {

  val spark = SparkSession.builder()
    .appName("Store")
    .master("local[*]")
    .getOrCreate()


  val sc = spark.sparkContext

  case class Store(
    state: String,
    location: String,
    address: String,
    latitude: Double,
    longitude: Double
  )

  def readStore(fileName: String) = {
    Source.fromFile(fileName)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map( values => Store(
        values(0),
        values(1),
        values(2),
        values(3).toDouble,
        values(4).toDouble)
      ).toList
  }


  val storeRdd: RDD[Store] = sc.parallelize(readStore("src/main/resources/chipotle_stores.csv"))

  storeRdd.foreach(println)

}
