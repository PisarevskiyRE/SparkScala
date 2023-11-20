package com.example
package repeat.ch3_3

import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Locale

object Channels extends App {

  val spark = SparkSession.builder()
    .appName("Channels")
    .master("local")
    .getOrCreate()


  val channelDf = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/channel.csv")

  channelDf.printSchema()
  channelDf.show(truncate = false)


  case class Channel(channel_name: String,city: String, country: String, created: String)

  import spark.implicits._

  val channelDs = channelDf.as[Channel]


  val cityNamesDs = channelDs.map( x => x.city.toUpperCase)




  /*расчет даты*/

  case class Age(chanelName: String, age: String)


  def toDate(date: String, dateFormat: String): Long = {
    val format = new SimpleDateFormat(dateFormat, Locale.ENGLISH)
    format.parse(date).getTime
  }

  def countChannelAge(channel: Channel): Age = {
    val age = (toDate("11/19/2023", "MM/dd/yyyy") - toDate(channel.created, "yyyy MMM dd")) / (1000*60*60*24)
    Age(channel.channel_name, age.toString)
  }


  channelDs.map(x => countChannelAge(x)).show

}
