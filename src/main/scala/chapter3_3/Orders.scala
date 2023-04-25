package chapter3_3

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row, SparkSession}


object Orders extends App {

  val spark = SparkSession
    .builder()
    .appName("TestSpark")
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


  val ordersSchema: StructType = Encoders.product[Order].schema

  import spark.implicits._

  val ordersDS = spark
    .createDataFrame(
      spark.sparkContext.parallelize(ordersData),
      ordersSchema
    ).as[Order]


  // 1
  def getTotalStats(orders: Dataset[Order]): (Double, Int) = {
    val stats: (Double, Int) = orders
      .map(order => (order.priceEach * order.quantity, order.quantity))
      .reduce((a,b)=> (a._1 + b._1, a._2 + b._2))

    (stats._1, stats._2)
  }

  val (price, orderQuantity) = getTotalStats(ordersDS)

  //2
  case class CustomerInfo(
     customerId: Int,
     priceTotal: Double
   )

  val infoDS: Dataset[CustomerInfo] = ordersDS
    .groupByKey(_.customerId)
    .mapGroups { (id, orders) => {
      val priceTotal = orders.map(order => order.priceEach * order.quantity).sum.round
      val ordersTotal = orders.size // будет 0, тк после вычисления priceTotal буфер пуст

      CustomerInfo(
        id,
        priceTotal
      )
    }
    }

  infoDS.show()

  //3
  case class Sales(
    customer: String,
    product: String,
    price: Double,
  )

  case class Customer(
   id: Int,
   email: String,
   orders: Seq[Int])

  case class Order2(
    orderId: Int,
    product: String,
    quantity: Int,
    priceEach: Double)


  val customerData: Seq[Row] = Seq(
    Row(1, "Bob@example.com", Seq()),
    Row(2, "alice@example.com", Seq(1, 3)),
    Row(3, "Sam@example.com", Seq(2, 4))
  )

  val ordersData2: Seq[Row] = Seq(
    Row(1, "USB-C Charging Cable", 3, 11.29),
    Row(2, "Google Phone", 1, 600.33),
    Row(2, "Wired Headphones", 2, 11.90),
    Row(3, "AA Batteries (4-pack)", 4, 3.85),
    Row(4, "Bose SoundSport Headphones", 1, 99.90),
    Row(4, "20in Monitor", 1, 109.99)
  )


  def toDS[T <: Product : Encoder](data: Seq[Row], schema: StructType): Dataset[T] =
    spark
      .createDataFrame(
        spark.sparkContext.parallelize(data),
        schema
      ).as[T]


}
