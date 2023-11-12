package com.example
package other

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}



object Test extends App {

  case class Person(id: Int, name: String, age: Int)
  case class Salary(id: Int, month: String, salary: Double)



  val spark = SparkSession
    .builder()
    .appName("test")
    .master("local")
    .getOrCreate()


  val personDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .csv("src/main/resources/person.csv")


  val salaryDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .csv("src/main/resources/salary.csv")



  import spark.implicits._

  val personDS = personDF.as[Person]
  val salaryDS = salaryDF.as[Salary]


  val joinCondition = personDS.col("id") === salaryDS.col("id")


  val joinedDS = personDF.as("a").join(salaryDS.as("b"), joinCondition, "inner")


  val metric1 = joinedDS
    .withColumn("AvgSalaryByPerson", avg("salary").over(Window.partitionBy("a.id")))
    .where(
      col("salary") > col("AvgSalaryByPerson")
    )


  metric1.show()


 val ageGroups =
   Seq(
     (30, 34, "30-34"),
     (35, 40, "34-40"),
     (41, Int.MaxValue, "40+")
   )

  val personWithGroup: DataFrame = personDS
    .withColumn("ageGroup",
      when(col("age").between(ageGroups(0)._1, ageGroups(0)._2), ageGroups(0)._3)
        .when(col("age").between(ageGroups(1)._1, ageGroups(1)._2), ageGroups(1)._3)
        .when(col("age").between(ageGroups(2)._1, ageGroups(2)._2), ageGroups(2)._3)
        .otherwise("unknown")
    )

  val metric2 =
    personWithGroup
      .join(salaryDF, joinCondition, "inner")
      .groupBy("ageGroup")
      .agg(
        min("salary").alias("minSalary"),
        max("salary").alias("maxSalary"),
        avg("salary").alias("avgSalary")
      )

  metric2.show()


  val jsonStats = struct(
    col("ageGroup"),
    col("minSalary").alias("minSalary"),
    col("maxSalary").alias("maxSalary"),
    col("avgSalary").alias("avgSalary")
  ).as("stats_json")

  val metric3 = metric2
    .withColumn("json", to_json(jsonStats))

  metric3.show()


  val jsonStatsSchema = new StructType()
    .add("ageGroup", StringType)
    .add("minSalary", DoubleType)
    .add("maxSalary", DoubleType)
    .add("avgSalary", DoubleType)

  val metric4 = metric3
    .withColumn("upvt_json", from_json(col("json"), jsonStatsSchema))
    .select(
      col("ageGroup"),
      col("upvt_json.minSalary"),
      col("upvt_json.maxSalary"),
      col("upvt_json.avgSalary")
    )

  metric4.show()

}

