package com.example
package repeat.other

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test4 extends App {

  case class Person(id: Int, name: String, age: Int)
  case class Salary(id: Int, month: String, salary: Double)


  val spark = SparkSession
    .builder()
    .appName("Тест4")
    .master("local")
    .getOrCreate()


  import spark.implicits._

  val personsDF =
    Seq(
      Person(1, "a",30),
      Person(2, "b",35),
      Person(3, "c",40),
      Person(4, "d",37)
    ).toDF()



  val salaryDF = Seq(
    Salary(1,"1",1000.0),
    Salary(1,"2",1100.0),
    Salary(1,"3",1300.0),
    Salary(2, "1", 1000.0),
    Salary(2, "2", 1100.0),
    Salary(2, "3", 1300.0),
    Salary(3, "1", 1000.0),
    Salary(3, "2", 1100.0),
    Salary(3, "3", 1300.0),
    Salary(4, "1", 1000.0),
    Salary(4, "2", 1100.0),
    Salary(4, "3", 1300.0),
  ).toDF()

//  personsDF.show
//  personsDF.printSchema

  //salaryDF.show
  //salaryDF.printSchema


  val joinCondition = col("a.id") === col("b.id")

  val joinedDF = personsDF.as("a").join(salaryDF.as("b"), joinCondition, "inner")


  //joinedDF.show

  val metric1 = joinedDF
    .withColumn("avgSalary",
      avg("b.salary").over(Window.partitionBy("a.id")))
    .where( col("b.salary") > col("avgSalary"))
    .select(
      "a.id",
      "b.month",
      "b.salary"
    )

  metric1.show()

  val ageGroup = Seq(
    (30,35,"a"),
    (35,40,"b"),
    (40,45,"c")
  )

  val metric2 = joinedDF
    .withColumn( "group",
      when(col("a.age").between(ageGroup(0)._1, ageGroup(0)._2), ageGroup(0)._3)
        .when(col("a.age").between(ageGroup(1)._1, ageGroup(1)._2), ageGroup(1)._3)
        .when(col("a.age").between(ageGroup(2)._1, ageGroup(2)._2), ageGroup(2)._3)
    )
    .groupBy("group")
    .agg(
      min("b.salary").as("min"),
      max("b.salary").as("max"),
      avg("b.salary").as("avg"),
    )

  metric2.show


  val jsonStats = struct(
    col("group"),
    col("min").alias("min"),
    col("max").alias("max"),
    col("avg").alias("avg")
  ).as("stats_json")

  val metric3 = metric2
    .withColumn("Json", to_json(jsonStats))

  metric3.show()


  val jsonStatsSchema = new StructType()
    .add("group", StringType)
    .add("min", DoubleType)
    .add("max", DoubleType)
    .add("avg", DoubleType)

  val metric4 = metric3
    .withColumn("upvt_json", from_json(col("json"), jsonStatsSchema))
    .select(
      col("group"),
      col("upvt_json.min"),
      col("upvt_json.max"),
      col("upvt_json.avg")
    )

  metric4.show()
}
