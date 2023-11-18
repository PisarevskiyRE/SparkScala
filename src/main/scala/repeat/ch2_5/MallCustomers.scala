package com.example
package repeat.ch2_5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object MallCustomers extends App {

  val spark = SparkSession.builder()
    .appName("MallCustomers")
    .master("local")
    .getOrCreate()


  val mallCustomersDf = spark.read
    .format("csv")
    .options(Map(
      "inferSchema" -> "false",
      "header" -> "true",
      "sep" -> ",",
      "mode" -> "permissive", //ошибки в null
      "path" -> "src/main/resources/mall_customers.csv",
      "nullValue" -> "n/a"
    ))
    .load()


  val mallCustomersFixDf = mallCustomersDf
    .withColumn("trueAge",
      col("Age").cast(IntegerType) + 2
    )
    .drop(col("Age"))
    .withColumnRenamed("trueAge","Age")



  val groupedDf = mallCustomersFixDf
    .where(
      col("Age").between(30,35)
    )
    .groupBy(
      col("Gender"), col("Age")
    )
    .agg(
      round( avg(col("Annual Income (k$)"))
      ,1)
        .as("avg")
    )
    .select(
      col("Age"),
      col("Gender"),
      col("avg")
    )
    .orderBy(col("Age"),
      col("Gender"))
    .withColumn("GGG",
      when(col("Gender") ==="Female", 1)
        .otherwise(0)
    )









  groupedDf.show(truncate = false)
  groupedDf.printSchema()
  groupedDf.explain()

}
