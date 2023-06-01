package chapter6_4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Over extends App {
  val spark = SparkSession
    .builder()
    .appName("BroadcastJoin")
    .master("local[*]")
    .getOrCreate()

  // всегда как с большим объемом данных
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


  val data = Seq(
    ("A", 1, "20000"),
    ("A", 2, "1000"),
    ("B", 2, "3000"),
    ("B", 3, "3000"),
    ("B", 5, "5000"),
    ("C", 6, "70000"),
    ("D", 6, "90000"),
    ("D", 7, "10"),
    ("E", 8, "1300"),
  )
  import spark.implicits._

  val df = data.toDF("user", "month", "amount")

  /**
   * через JOIN
   */
  val aggDF = df
    .groupBy("user")
    .agg(
      max("amount").as("maxAmount")
    )

  val joinedDF = df.join(aggDF, "user")

  joinedDF.show()
  joinedDF.explain()

  /**
   * оконные функции
   */

  val window = Window.partitionBy("user")
  val windowDF = df
    .withColumn(
      "maxAmount",
      max("amount").over(window)
    )
  windowDF.show()
  windowDF.explain()


}
