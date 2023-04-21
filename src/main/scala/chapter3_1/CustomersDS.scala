package chapter3_1

import org.apache.spark.sql.{DataFrame, SparkSession}

object CustomersDS {

  val spark = SparkSession
    .builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()


  //имена полей в case классе должны совпадать с используемыми в файле названиями колонок:
  case class Customer(
                       name: String,
                       surname: String,
                       age: Int,
                       occupation: String,
                       customer_rating: Double
                     )


  val customersDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .csv("src/main/resources/customers.csv")


  import spark.implicits._
  val customersDS = customersDF.as[Customer]


  customersDS.filter(x=>x.name != "Bob").show
  //customersDS.filter(x => x.name != "Bob").show(20)




}
