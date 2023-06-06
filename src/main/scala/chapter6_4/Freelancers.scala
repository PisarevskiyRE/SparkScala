package chapter6_4

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Freelancers extends App{

  val spark = SparkSession
    .builder()
    .appName("Freelancers")
    .master("local[*]")
    .getOrCreate()

  // всегда как с большим объемом данных
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  val freelancersDF: DataFrame =
    spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("src/main/resources/freelancers.csv")

  val offersDF: DataFrame =
    spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("src/main/resources/offers.csv")

  import spark.implicits._


  val freelancersWithKeyDF = freelancersDF
    .withColumn("concatId", concat($"category", $"city"))
    .select($"id", $"concatId", $"experienceLevel")
    .distinct()  // без дистинкта не работает
    .repartition(8)


  val offersWithKeyDF = offersDF
    .withColumn("concatId", concat($"category", $"city"))
    .select($"concatId", $"experienceLevel", $"price")
    .distinct() // без дистинкта не работает
    .repartition(8)


  val resultDF = freelancersWithKeyDF
    .join(
      offersWithKeyDF,
      freelancersWithKeyDF("concatId") === offersWithKeyDF("concatId") &&
      abs(freelancersWithKeyDF.col("experienceLevel") - offersWithKeyDF.col("experienceLevel")) <= 1
    )
    .select("id", "price")


  val resultWithAvgDF = resultDF.groupBy("id")
    .agg(avg("price").as("avgPrice"))

  resultWithAvgDF.explain()
  resultWithAvgDF.show()

  System.in.read()
}
