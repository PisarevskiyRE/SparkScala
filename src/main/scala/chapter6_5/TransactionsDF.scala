package chapter6_5

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object TransactionsDF extends App{

  val spark = SparkSession
    .builder()
    .appName("TransactionsDF")
    .master("local[*]")
    .getOrCreate()

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  def read(file: String) = {
    spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(file)
  }

  val transactionsDF: DataFrame = read("src/main/resources/transactions.csv")
  val codesDF: DataFrame = read("src/main/resources/country_codes.csv")

  import spark.implicits._

  /**
   * Решил что если четко задан критерий
   * то будет уместно фильтровать по коду как литерал
   * не прибегая к Join и бродкасту
   */

  def getCountryId(country: String, df: DataFrame): Int = {
    df.select($"CountryCode")
      .where($"Country" === country)
      .first()
      .getInt(0)
  }

  val unitedKingdomId: Int = getCountryId("United Kingdom", codesDF)


  def filterByDate(date: String)(df: DataFrame): DataFrame ={
    df.filter($"Date" === date)
  }

  def filterByCountryCode(countryCode: Int)(df: DataFrame): DataFrame = {
    df.filter($"CountryCode" === countryCode)
  }

  def filterByQuantity(quantity: Int)(df: DataFrame): DataFrame = {
    df.filter($"Quantity" > quantity)
  }


  val transactionsOnDateDF = transactionsDF
    .select(
      $"TransactionNo",
      $"Date",
      $"CountryCode",
      $"ProductName",
      $"Quantity")
    .transform(filterByDate("2018-12-01"))
    .transform(filterByCountryCode(unitedKingdomId))
    .transform(filterByQuantity(0))

  val mostFrequentProduct = transactionsOnDateDF
    .groupBy("ProductName")
    .agg(sum("Quantity").alias("TotalQuantity"))
    .orderBy(desc("TotalQuantity"))

  val avgItemsPerTransaction = transactionsOnDateDF
    .groupBy("TransactionNo")
    .agg(sum("Quantity").alias("TotalQuantity"))
    .agg(avg("TotalQuantity").alias("AvgItemsPerTransaction"))

  val mostFrequentProductResult = mostFrequentProduct.first().getString(0)
  val avgItemsPerTransactionResult = avgItemsPerTransaction.first().getDouble(0)

  println(s"Наиболее часто покупаемый товар -> $mostFrequentProductResult")
  println(s"В среднем покупают за транзакцию -> $avgItemsPerTransactionResult.round")

  mostFrequentProduct.explain()
  avgItemsPerTransaction.explain()

  /**
   * правильный ответ
   *
   * -> Namaste Swagat Incense
   * -> 200
   */

  System.in.read()
}
