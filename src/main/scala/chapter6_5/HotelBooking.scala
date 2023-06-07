package chapter6_5

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object HotelBooking extends App{
  val spark = SparkSession
    .builder()
    .appName("HotelBooking")
    .master("local[*]")
    .getOrCreate()


  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


  def read(file: String) = {
    spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(file)
  }

  val codesDF: DataFrame = read("src/main/resources/cancel_codes.csv")

  val bookingsDF: DataFrame = read("src/main/resources/hotel_bookings.csv")

  import spark.implicits._

  val codesWIthConvertDF = codesDF
    .select(
      $"id",
      $"is_cancelled"
    )
    .withColumn("is_cancelled_as_bool",
        when($"is_cancelled" === "no", "0")
        .when($"is_cancelled" === "yes", "1"))


  val bookingsWIthConvertDF = bookingsDF
    .select(
      $"is_canceled",
      $"reservation_status"
    )
    .withColumn("reservation_status_as_bool",
      when($"reservation_status" === "Canceled", "1")
        .when($"reservation_status" === "Check-Out", "0"))


  val joinedDF2 = bookingsWIthConvertDF.join(broadcast(codesWIthConvertDF), bookingsWIthConvertDF("is_canceled") === codesWIthConvertDF("id"))
    .filter($"is_cancelled_as_bool" =!= $"reservation_status_as_bool")

  println("Количество ошибок => " + joinedDF2.count)

  joinedDF2.explain()

  System.in.read()
}
