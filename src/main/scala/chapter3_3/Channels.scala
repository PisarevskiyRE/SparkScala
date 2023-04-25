package chapter3_3

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.util.Locale

object Channels extends App{

  val spark = SparkSession
    .builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()


  val channelsDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/channel.csv")

  case class Channel(
                      channel_name: String,
                      city: String,
                      country: String,
                      created: String,
                    )


  import spark.implicits._

  val channelsDS = channelsDF.as[Channel]


  // 1 лямбда(map, flatmap и тд)
  val cityNamesDF = channelsDF
    .select(
      upper($"city").as("city")
    )

  val cityNamesDS =
    channelsDS.map(ch => ch.city.toUpperCase)


  // 2 работа с датами
  channelsDF
    .withColumn("today", to_date(lit("04/19/2022"), "MM/dd/yyyy"))
    .withColumn("actual_date", to_date($"created", "yyyy MMM dd"))
    .withColumn("channel_age",
      datediff($"today", $"actual_date"))
    .show




  case class Age(
                  channel_name: String,
                  age: String,
                )

  //кастомный энкодер

  implicit val encoder: ExpressionEncoder[Age] = ExpressionEncoder[Age]


  import java.text.SimpleDateFormat

  def toDate(date: String, dateFormat: String): Long = {
    val format = new SimpleDateFormat(dateFormat, Locale.ENGLISH)
    format.parse(date).getTime
  }

  def countChannelAge(channel: Channel): Age = {
    val age = (toDate("04/19/2022", "MM/dd/yyyy") - toDate(channel.created, "yyyy MMM dd")) / (1000 * 60 * 60 * 24)
    Age(channel.channel_name, age.toString)
  }


  val ageDS = channelsDS.map(channel => countChannelAge(channel))

  ageDS.show


  val joinedDS: Dataset[(Channel, Age)] = channelsDS
    .joinWith(ageDS, channelsDS.col("channel_name") === ageDS.col("channel_name"))

  joinedDS.show

}
