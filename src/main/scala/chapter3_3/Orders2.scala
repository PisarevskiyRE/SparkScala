package chapter3_3


import org.apache.spark.sql._
import org.apache.spark.sql.functions.array_contains
import org.apache.spark.sql.types.StructType


object Orders2 extends App {

  val spark = SparkSession
    .builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()


  case class Customer(
   id: Int,
   email: String,
   orders: Seq[Int])

  case class Order(
    orderId: Int,
    product: String,
    quantity: Int,
    priceEach: Double)


  val customerData: Seq[Row] = Seq(
    Row(1, "Bob@example.com", Seq()),
    Row(2, "alice@example.com", Seq(1, 3)),
    Row(3, "Sam@example.com", Seq(2, 4))
  )

  val ordersData: Seq[Row] = Seq(
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

  import spark.implicits._


  val ordersSchema: StructType = Encoders.product[Order].schema
  val customerSchema: StructType = Encoders.product[Customer].schema


  val customersDS = toDS[Customer](customerData, customerSchema)
  val ordersDS = toDS[Order](ordersData, ordersSchema)


  val joinedDS: Dataset[(Customer, Order)] = customersDS
    .joinWith(
      ordersDS,
      array_contains(customersDS.col("orders"), ordersDS.col("orderId")),
      "outer"
    )

  joinedDS.show(false)


  case class Sales(
        customer: String,
        product: String,
        price: Double,
      )


  val salesDS: Dataset[Sales] = joinedDS
    .filter(record => record._1.orders.nonEmpty)
    .map(record =>
      Sales(
        record._1.email.toLowerCase(),
        record._2.product,
        record._2.quantity * record._2.priceEach
      )
    )

  salesDS.show()


  val salesDS2: Dataset[Sales] = joinedDS
    .map(record => record._1.orders.isEmpty match {
      case false => Sales(
        record._1.email.toLowerCase(),
        record._2.product,
        record._2.quantity * record._2.priceEach)
      case _ => Sales(
        record._1.email.toLowerCase(),
        "-",
        0.0)
    })

  salesDS2.show()
}
