package chapter3_4

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions._
import java.sql.Date
import java.time.LocalDate
import java.time.temporal.ChronoUnit

object Cars extends App {

  val spark = SparkSession
    .builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()

  val carsDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .csv("src/main/resources/cars.csv")


  import spark.implicits._

  case class Car(
    id: Int, //надеямся что id будет у всех
    price: Option[Double],
    brand: Option[String],
    typeField: Option[String],
    mileage: Option[Double],
    color: Option[String],
    date_of_purchase: Option[Date], // def DATE: Encoder[java.sql.Date] = ExpressionEncoder()
  )
  //молимся что не появится новых форматов дат(аминь)
  val dateFormat1 = "yyyy MMM dd"
  val dateFormat2 = "yyyy MM dd"

  // почистим данные перед работой
  //*** можно было сделать коалиском по все форматам через withColumn


  val carClearDF = carsDF
    .select(
      $"id",
      $"price",
      $"brand",
      $"type".as("typeField"),
      $"mileage",
      $"color",
        when(
          length(
            regexp_replace($"date_of_purchase", "-", " ").as("date_of_purchase")
          ) === 11,
            to_date(regexp_replace($"date_of_purchase", "-", " ").as("date_of_purchase") ,dateFormat1))
        .when(
          length(
            regexp_replace($"date_of_purchase", "-", " ").as("date_of_purchase")
          ) === 10,
            to_date(regexp_replace($"date_of_purchase", "-", " ").as("date_of_purchase") ,dateFormat2))
        .otherwise(null).as("date_of_purchase")
    ).
    na.fill(0, Seq("mileage"))

  val carsDS: Dataset[Car] = carClearDF.as[Car]



// пытаемся делать функционально
  case class years_since_purchase(carId: Int, value: Long)

  implicit val encoder: ExpressionEncoder[years_since_purchase] = ExpressionEncoder[years_since_purchase]

  def cars_avg_mileage(cars: Dataset[Car]): Option[Double] = {
    val carsAvg = cars
      .map(c => c.mileage)
      .reduce((a,b) => (Some(a.getOrElse(0.0) + b.getOrElse(0.0))))
    carsAvg
  }

  val years_since_purchaseDS: Dataset[years_since_purchase] =  carsDS.map {
    car => (car.id, car.date_of_purchase) match {
      case (car.id, Some(x)) =>
        years_since_purchase(car.id,
          ChronoUnit.YEARS.between(x.toLocalDate,Date.valueOf(LocalDate.now()).toLocalDate )
        )
      case (id, None) =>
        years_since_purchase(id, 0)
    }
  }

  val cars_avg_mileage_val: Option[Double] = cars_avg_mileage(carsDS)

  val carsResultDS: Dataset[(Car, years_since_purchase)] = carsDS
    .joinWith(years_since_purchaseDS,carsDS("id") === years_since_purchaseDS("carId"))


  carsResultDS
    .select(
      $"_1.id",
      $"_2.value".as("years_since_purchase")
    )
    .withColumn("avg_mileage", lit(cars_avg_mileage_val.getOrElse(0.0)))
    .show


  /*
  Результат
  +---+--------------------+-----------+
  | id|years_since_purchase|avg_mileage|
  +---+--------------------+-----------+
  |  0|                  14|   786541.0|
  |  1|                  12|   786541.0|
  |  2|                   4|   786541.0|
  |  3|                   9|   786541.0|
  |  4|                   4|   786541.0|
  |  5|                   5|   786541.0|
  |  6|                  12|   786541.0|
  |  7|                   5|   786541.0|
  +---+--------------------+-----------+
   */
}
