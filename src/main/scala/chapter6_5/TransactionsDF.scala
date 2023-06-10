package chapter6_5

import org.apache.spark.sql.expressions.Window
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


  val UnitedKingdomId: Int = codesDF
    .select($"CountryCode")
    .where($"Country" === "United Kingdom")
    .first()
    .getInt(0)

  val transactionsOnDateDF = transactionsDF
    .select(
      $"TransactionNo",
      $"Date",
      $"CountryCode",
      $"ProductName",
      $"Quantity")
    .where($"Date" === "2018-12-01"
      && $"CountryCode" === UnitedKingdomId
      && $"Quantity" >= 0
    )



  val mostFrequentProduct = transactionsOnDateDF.groupBy("ProductName")
    .agg(sum("Quantity").alias("TotalQuantity"))
    .orderBy(desc("TotalQuantity"))
    .select("ProductName")
//    .first()
//    .getString(0)

  val avgItemsPerTransaction = transactionsOnDateDF.groupBy("TransactionNo")
    .agg(sum("Quantity").alias("TotalQuantity"))
    .agg(avg("TotalQuantity").alias("AvgItemsPerTransaction"))
//    .first()
//    .getInt(0)



  println( mostFrequentProduct.first().getString(0))
  println( avgItemsPerTransaction.first().getDouble(0))


  mostFrequentProduct.explain()
  avgItemsPerTransaction.explain()


  /**
   * правильный ответ
   * Namaste Swagat Incense	600
   *
   * 199
   *
   */


  System.in.read()
}
