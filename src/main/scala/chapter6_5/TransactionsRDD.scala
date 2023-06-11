package chapter6_5

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession}

object TransactionsRDD extends App{
  /**
   * на всякий случай в sql server проверил,
   * а то был не уверен в правильности написания в функциональом стие(мало опыта)
   *
   * -- наиболее часто покупаемый товар (ProductName)
   * SELECT top 1
   * [ProductName]
   * ,Sum(cast([Quantity] as int))
   * FROM [Hermes-Items].[dbo].[transactions]
   * where [CountryCode] = 36 and [Date] = '2018-12-01' and cast([Quantity] as int) >= 0
   * group by [ProductName]
   * order by Sum(cast([Quantity] as int)) desc
   *
   *
   * -- сколько товаров в среднем посетители приобретают за одну транзакцию
   * SELECT
   * [TransactionNo]
   * ,avg(Sum(cast([Quantity] as int))) over ()
   * FROM [Hermes-Items].[dbo].[transactions]
   * where [CountryCode] = 36 and [Date] = '2018-12-01' and cast([Quantity] as int) >= 0
   * group by [TransactionNo]
   *
   */


  val spark = SparkSession
    .builder()
    .appName("TransactionsRDD")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  case class Transaction( TransactionNo: String,
                          Date: String,
                          ProductName: String,
                          Quantity: Int,
                          CountryCode: Int)

  case class Country( Country: String,
                      CountryCode: Int)

  val transactionsRDD: RDD[Transaction] = sc.textFile("src/main/resources/transactions.csv")
    .map(line => line.split(","))
    .filter(values => values(0) != "TransactionNo")
    .map(values => Transaction(
      values(0),
      values(1),
      values(3),
      values(5).toInt,
      values(7).toInt
    ))

  val CountryRDD: RDD[Country] = sc.textFile("src/main/resources/country_codes.csv")
    .map(line => line.split(","))
    .filter(values => values(0) != "Country")
    .map(values => Country(
      values(0),
      values(1).toInt
    ))

//  transactionsRDD.take(10).foreach(println)
//  CountryRDD.take(10).foreach(println)

  val UnitedKingdomId: Int = CountryRDD
    .filter(  x =>
      x.Country == "United Kingdom")
    .first()
    .CountryCode

  val transactionOnDateRDD = transactionsRDD
    .filter(x =>
      x.CountryCode == UnitedKingdomId
      && x.Date == "2018-12-01"
      && x.Quantity > 0)

  val mostFrequentProductResult: String = transactionOnDateRDD
    .map(x => (x.ProductName, x.Quantity))
    .reduceByKey(_ + _)
    .sortBy(-_._2)
    .first()
    ._1

  println(s"Наиболее часто покупаемый товар -> $mostFrequentProductResult")

  val trnsactionCnt: Long = transactionOnDateRDD
    .groupBy(x => x.TransactionNo)
    .count()

  val avgItemsPerTransactionResult: Double = transactionOnDateRDD
    .groupBy (x => x.TransactionNo)
    .mapValues( x =>
      x.map(_.Quantity).sum
    ).map(_._2).sum() / trnsactionCnt

  println(s"В всреднем покупают за транзакцию -> ${avgItemsPerTransactionResult.round}")

  /**
   * правильный ответ
   *
   * -> Namaste Swagat Incense
   * -> 200
   */

  /*
  Оптимизации:
  - загрузка только нужных полей
  - фильтрация литералами
  - исключение join
  - строгая типизация
  - * расчитывать только то что необходимо для ответа
  */
  System.in.read()
  
}
