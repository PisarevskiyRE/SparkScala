package chapter3_2

import org.apache.spark.sql.{DataFrame, SparkSession}

object CustomersWithNullsDS{

  val spark = SparkSession
    .builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()


  val customersDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/customers_with_nulls.csv")

  case class Customer(
                        name: String,
                        age: Option[Int],
                        customer_rating: Option[Double])


  import spark.implicits._

  val customersDS = customersDF.as[Customer]

  customersDS
    .filter(customer => customer.customer_rating.getOrElse(0.0) > 4.0)
    .show()

}
