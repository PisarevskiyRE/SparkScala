package chapter4_2

import org.apache.spark.rdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.time.{LocalDate, YearMonth}
import java.time.format.DateTimeFormatter
import java.util.Calendar
import scala.io.Source
import scala.util.Try
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg

import scala.io.Source


object Avocado extends App {

  val spark = SparkSession.builder()
    .appName("RDDs")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  case class Avocado(
    id: Int,
    date: String,
    avgPrice: Double,
    volume: Double,
    year: String,
    region: String
  )


  def readAvocados(filename: String) =
    Source.fromFile(filename)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(values => Avocado(
        values(0).toInt,
        values(1),
        values(2).toDouble,
        values(3).toDouble,
        values(12),
        values(13))
      ).toList

  val AvocadoRDD: RDD[Avocado] = sc.parallelize(readAvocados("src/main/resources/avocado.csv"))

  //0 загрузили
  AvocadoRDD.foreach(println)


  //1 подсчитайте количество уникальных регионов (region), для которых представлена статистика
  val regionUnicRDD: RDD[String] = AvocadoRDD.map(_.region).distinct()
  regionUnicRDD.foreach(println)

  // 2 выберите и отобразите на экране все записи о продажах авокадо, сделанные после 2018-02-11
  val getAvocadoAfterDate: RDD[Avocado] = AvocadoRDD.filter(x =>
    {
      val dateFormat1 = new SimpleDateFormat("yyyy-MM-dd")
      val dateFormat2 = new SimpleDateFormat("yyyy-MM-dd")

      if (x.date.isEmpty) false
      else dateFormat1.parse(x.date).after(dateFormat2.parse("2018-02-11"))
    }
  )
  getAvocadoAfterDate.foreach(println)

  //3 найдите месяц, который чаще всего представлен в статистике
  val mostMonth: Int = AvocadoRDD.groupBy(x => {
    val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val defaultMonth = 1
    Try(LocalDate.parse(x.date, dateFormatter).getMonthValue).getOrElse(defaultMonth)
  })
  .map(x => (x._1, x._2.size))
  .reduce((x, y) => if (x._2 > y._2) x else y)
  ._1

  println(s"avocado month = $mostMonth")

  //4 найдите максимальное и минимальное значение avgPrice
  import spark.implicits._
  implicit val avocadoOrderingAvgPrice: Ordering[Avocado] =
    Ordering.fromLessThan[Avocado]((a: Avocado, b: Avocado) => a.avgPrice < b.avgPrice)

  val avocadoOrderingAvgPriceMax = AvocadoRDD.max()
  val avocadoOrderingAvgPriceMin = AvocadoRDD.min()

  println(s"max = $avocadoOrderingAvgPriceMax")
  println(s"min = $avocadoOrderingAvgPriceMin")


  //5 отобразите средний объем продаж (volume) для каждого региона (region)
  val avgVolume = AvocadoRDD
    .groupBy(_.region)
    .mapValues(iter => iter.map(_.volume).sum / iter.size)

  avgVolume.foreach(println)


}
