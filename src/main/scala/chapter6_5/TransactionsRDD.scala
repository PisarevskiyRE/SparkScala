package com.example
package chapter6_5

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object TransactionsRDD extends App{

  val spark = SparkSession
    .builder()
    .appName("TransactionsDF")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  case class Transaction(
                          TransactionNo: String,
                          Date: String,
                          ProductNo: String,
                          ProductName: String,
                          Price: Double,
                          Quantity: Int,
                          CustomerNo: String,
                          CountryCode: Int
                        )

  case class Country(
                      Country: String,
                      CountryCode: Int
                    )

  val transactionsRDD = sc.textFile("src/main/resources/transactions.csv")
    .map(line => line.split(","))
    .filter(values => values(0) != "TransactionNo")
    .map(values => Transaction(
      values(0),
      values(1),
      values(2),
      values(3),
      values(4).toDouble,
      values(5).toInt,
      values(6),
      values(7).toInt
    ))

  val CountryRDD = sc.textFile("src/main/resources/country_codes.csv")
    .map(line => line.split(","))
    .filter(values => values(0) != "Country")
    .map(values => Country(
      values(0),
      values(1).toInt
    ))

//  transactionsRDD.take(10).foreach(println)
//  CountryRDD.take(10).foreach(println)


  /**
   * == Physical Plan ==
   * AdaptiveSparkPlan isFinalPlan=false
   * +- Project [ProductName#20]
   * +- Sort [TotalQuantity#72L DESC NULLS LAST], true, 0
   * +- Exchange rangepartitioning(TotalQuantity#72L DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=237]
   * +- HashAggregate(keys=[ProductName#20], functions=[sum(Quantity#22)])
   * +- Exchange hashpartitioning(ProductName#20, 200), ENSURE_REQUIREMENTS, [plan_id=234]
   * +- HashAggregate(keys=[ProductName#20], functions=[partial_sum(Quantity#22)])
   * +- Project [ProductName#20, Quantity#22]
   * +- Filter (((isnotnull(Date#18) AND isnotnull(CountryCode#24)) AND (Date#18 = 2018-12-01 00:00:00)) AND (CountryCode#24 = 36))
   * +- FileScan csv [Date#18,ProductName#20,Quantity#22,CountryCode#24] Batched: false, DataFilters: [isnotnull(Date#18), isnotnull(CountryCode#24), (Date#18 = 2018-12-01 00:00:00), (CountryCode#24 ..., Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/D:/!Work/Scala/Projects/SparkScala/src/main/resources/transactio..., PartitionFilters: [], PushedFilters: [IsNotNull(Date), IsNotNull(CountryCode), EqualTo(Date,2018-12-01 00:00:00.0), EqualTo(CountryCod..., ReadSchema: struct<Date:timestamp,ProductName:string,Quantity:int,CountryCode:int>
   *
   */
/*
    case class ProductCount(ProductName: String,
                            Quantity: Int)

  val UnitedKingdomId: Int = CountryRDD.filter(x =>
    x.Country == "United Kingdom"
  ).first().CountryCode

  println(UnitedKingdomId)

  val transactionsOnDateRDD = transactionsRDD.filter(x =>
    x.CountryCode == 36 && x.Date == "2018-12-01"
  )
    .map( x=>
    ProductCount(
      x.ProductName,
      x.Quantity
    )
  )



  val mostPopulare = transactionsOnDateRDD
    .groupBy(_.ProductName)
    .mapValues(x=> x.map(_.Quantity).sum / x.size)


  mostPopulare.foreach(println)
*/


  val mostFrequentProduct = transactionsRDD
    .filter(x => x.CountryCode == 36 && x.Date == "2018-12-01")
    .map(x => (x.ProductName, x.Quantity)) // Создание пары (ProductName, 1)
    .reduceByKey(_ + _) // Суммирование количества покупок для каждого товара
    .sortBy(-_._2) // Сортировка по убыванию количества покупок
    .first() // Получение первого элемента (наиболее часто покупаемый товар)

  println(s"Наиболее часто покупаемый товар: ${mostFrequentProduct._1}")

  // Расчет среднего количества товаров в одной транзакции (TransactionNo)
//  val averageQuantityPerTransaction= transactionsRDD
//    .filter(x => x.CountryCode == 36 && x.Date == "2018-12-01")
//    .map(x => (x.TransactionNo, x.Quantity)) // Создание пары (ProductName, 1)
//    .reduceByKey(_ + _) // Суммирование количества покупок для каждого товара
//    .groupBy( )
//    .mapValues(x=> x.map(_._2).sum / x.size)




//  val averageQuantityPerTransaction2: RDD[(String, Double)] = transactionsRDD
//    .filter(x => x.CountryCode == 36 && x.Date == "2018-12-01" && x.Quantity >= 0)
//    .groupBy(_.TransactionNo) // Группировка транзакций по TransactionNo
//    .mapValues(transactions => {
//      val totalQuantity = transactions.map(_.Quantity).sum
//      val transactionCount = transactions.size
//      totalQuantity.toDouble / transactionCount // Расчет среднего количества товаров в каждой группе
//    })
  /**
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

  val cnt: Long = transactionsRDD
      .filter(x => x.CountryCode == 36 && x.Date == "2018-12-01" && x.Quantity >= 0)
      .groupBy(x => x.TransactionNo).count()

     println(cnt)


  val averageQuantityPerTransaction2 = transactionsRDD
    .filter(x => x.CountryCode == 36 && x.Date == "2018-12-01" && x.Quantity >= 0)
    .groupBy (x => x.TransactionNo)
    .mapValues( x =>
      x.map(_.Quantity).sum
    ).map(_._2).sum() / cnt




  //averageQuantityPerTransaction2.foreach(println)

  println(averageQuantityPerTransaction2)

  /**
   * правильный ответ
   * Namaste Swagat Incense	600
   *
   * 199
   *
   */

  /*val avgVolume = AvocadoRDD
    .groupBy(_.region)
    .mapValues(iter => iter.map(_.volume).sum / iter.size)*/

 // println(s"Среднее количество товаров в одной транзакции: $averageQuantityPerTransaction")
  /**
   * == Physical Plan ==
   * AdaptiveSparkPlan isFinalPlan=false
   * +- HashAggregate(keys=[], functions=[avg(TotalQuantity#82L)])
   * +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=266]
   * +- HashAggregate(keys=[], functions=[partial_avg(TotalQuantity#82L)])
   * +- HashAggregate(keys=[TransactionNo#17], functions=[sum(Quantity#22)])
   * +- Exchange hashpartitioning(TransactionNo#17, 200), ENSURE_REQUIREMENTS, [plan_id=262]
   * +- HashAggregate(keys=[TransactionNo#17], functions=[partial_sum(Quantity#22)])
   * +- Project [TransactionNo#17, Quantity#22]
   * +- Filter (((isnotnull(Date#18) AND isnotnull(CountryCode#24)) AND (Date#18 = 2018-12-01 00:00:00)) AND (CountryCode#24 = 36))
   * +- FileScan csv [TransactionNo#17,Date#18,Quantity#22,CountryCode#24] Batched: false, DataFilters: [isnotnull(Date#18), isnotnull(CountryCode#24), (Date#18 = 2018-12-01 00:00:00), (CountryCode#24 ..., Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/D:/!Work/Scala/Projects/SparkScala/src/main/resources/transactio..., PartitionFilters: [], PushedFilters: [IsNotNull(Date), IsNotNull(CountryCode), EqualTo(Date,2018-12-01 00:00:00.0), EqualTo(CountryCod..., ReadSchema: struct<TransactionNo:string,Date:timestamp,Quantity:int,CountryCode:int>
   *
   */




  System.in.read()
}
