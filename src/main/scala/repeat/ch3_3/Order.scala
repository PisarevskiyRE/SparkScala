package com.example
package repeat.ch3_3

import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}

object Order extends App {

  val spark = SparkSession.builder()
    .appName("Orders")
    .master("local")
    .getOrCreate()


  case class Order(
                    orderId: Int,
                    customerId: Int,
                    product: String,
                    quantity: Int,
                    priceEach: Double
                  )


  val ordersData: Seq[Row] = Seq(
    Row(1, 2, "USB-C Charging Cable", 3, 11.29),
    Row(2, 3, "Google Phone", 1, 600.33),
    Row(2, 3, "Wired Headphones", 2, 11.90),
    Row(3, 2, "AA Batteries (4-pack)", 4, 3.85),
    Row(4, 3, "Bose SoundSport Headphones", 1, 99.90),
    Row(4, 3, "20in Monitor", 1, 109.99)
  )

  val orderSchema = Encoders.product[Order].schema


  import spark.implicits._

  val ordersDs = spark.createDataFrame(
    spark.sparkContext.parallelize(ordersData),
    orderSchema
  ).as[Order]

  ordersDs.show()


  def getTotalStats(orders: Dataset[Order]): (Double, Int) = {
    val stats: (Double, Int) = orders
      .map(ord => (ord.priceEach * ord.quantity, ord.quantity))
      .reduce((a,b) => (a._1 + b._1, a._2 + b._2))

    stats
  }


  val (price, quantity) = getTotalStats(ordersDs)

  println((price, quantity))



  case class CustomerInfo(customerId : Int, priceTotal: Double)

  val custInfo: Dataset[CustomerInfo] = ordersDs
    .groupByKey( x=>x.customerId )
    .mapGroups((id, ord) => {
      val priceTotal = ord.map(o => o.priceEach * o.quantity).sum.round
      CustomerInfo(id, priceTotal)
    })

  custInfo.show
}
