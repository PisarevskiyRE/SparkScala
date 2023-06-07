package com.example
package chapter6_5

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object HotelBooking2 extends App{
  val spark = SparkSession
    .builder()
    .appName("HotelBooking")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  case class HotelBooking(hotel: String,
                          is_canceled: Int,
                          lead_time: Int,
                          arrival_date_year: Int,
                          arrival_date_month: String,
                          arrival_date_week_number: Int,
                          arrival_date_day_of_month: Int,
                          stays_in_weekend_nights: Int,
                          stays_in_week_nights: Int,
                          adults: Int,
                          children: String,
                          babies: Int,
                          meal: String,
                          country: String,
                          market_segment: String,
                          distribution_channel: String,
                          is_repeated_guest: Int,
                          previous_cancellations: Int,
                          previous_bookings_not_canceled: Int,
                          reserved_room_type: String,
                          assigned_room_type: String,
                          booking_changes: Int,
                          deposit_type: String,
                          agent: String,
                          company: String,
                          days_in_waiting_list: Int,
                          customer_type: String,
                          adr: Double,
                          required_car_parking_spaces: Int,
                          total_of_special_requests: Int,
                          reservation_status: String,
                          reservation_status_date: String)


  case class CancelCode(id: Int,
                        is_cancelled: String,
                        confirmation: String)


  val codesRDD = sc.textFile("src/main/resources/cancel_codes.csv")
    .map(line => line.split(","))
    .filter(values => values(0) != "id")
    .map(values => CancelCode(
      values(0).toInt,
      values(1),
      values(2)
      )
    )


  val bookingsRDD = sc.textFile("src/main/resources/hotel_bookings.csv")
    .map(line => line.split(","))
    .filter(values => values(0) != "hotel")
    .map(values => HotelBooking(
      values(0),
      values(1).toInt,
      values(2).toInt,
      values(3).toInt,
      values(4),
      values(5).toInt,
      values(6).toInt,
      values(7).toInt,
      values(8).toInt,
      values(9).toInt,
      values(10),
      values(11).toInt,
      values(12),
      values(13),
      values(14),
      values(15),
      values(16).toInt,
      values(17).toInt,
      values(18).toInt,
      values(19),
      values(20),
      values(21).toInt,
      values(22),
      values(23),
      values(24),
      values(25).toInt,
      values(26),
      values(27).toDouble,
      values(28).toInt,
      values(29).toInt,
      values(30),
      values(31)
    ))


  def convertIsCancelled(cancelled: String): Int = {
    if (cancelled == "yes") 1 else 0
  }

  def convertReservationStatus(status: String): Int = {
    if (status == "Canceled") 1 else 0
  }

  val codeResultRDD: RDD[(Int, Int)] = codesRDD.map { x =>
    (x.id, convertIsCancelled(x.is_cancelled))
  }



  val bookingsResultRDD: RDD[(Int, Int)] = bookingsRDD.map{ x =>
    (x.is_canceled , convertReservationStatus(x.reservation_status))
  }

  val joinedRDD: RDD[(Int, (Int, Int))] = bookingsResultRDD.join(codeResultRDD)


  val eroorsRDD = joinedRDD
    .filter { case (_, (reservationStatus, isCancelled)) =>
      reservationStatus != isCancelled
    }

  println("Количество ошибок => " + eroorsRDD.count)

  println(eroorsRDD.toDebugString)

  System.in.read()
}
