package com.example
package repeat.other

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.util.Random

object DF_G extends App {

  case class Person(id: Int, firstName: String, lastName: String, age: Int)
  case class Salary(personId: Int, month: Int, Salary: Double)

  val persons = (1 to 100).map( x =>
    Person(x, "FirstName_"+ x.toString, "LastName_"+ x.toString, Random.nextInt(30)+22)
  )

  val salary = (1 to 12).flatMap( month =>
    persons.map( pers => Salary(pers.id, month, Random.nextInt(1000)+1000))
  )

  //persons.foreach(println)
  //salary.foreach(println)

  val spark = SparkSession.builder()
    .appName("Test")
    .master("local")
    .getOrCreate()

  import spark.implicits._


  val personDf = persons.toDF().as("p")
  val salaryDf = salary.toDF().as("s")

  val joinCondition = col("id") === col("personId")
  val joinedDf = personDf.join(salaryDf, joinCondition, "inner")


  val metric1 = joinedDf.withColumn( "avg_salary",
    avg(col("Salary")).over(Window.partitionBy(col("personId")))
  )
    .where(col("avg_salary") < col("Salary"))
    .select("p.*", "s.month")

  //metric1.show(truncate = false)

  val groups = Seq(
    (0, 25, "a"),
    (26, 35, "b"),
    (36, 100, "c"),
  )

  val metric2 = joinedDf.withColumn("group",
    when(col("age").between(groups(0)._1, groups(0)._2), groups(0)._3)
      .when(col("age").between(groups(1)._1, groups(1)._2), groups(1)._3)
      .when(col("age").between(groups(2)._1, groups(2)._2), groups(2)._3)
  )
    .groupBy(col("group"))
    .agg(
      min(col("salary")).as("min"),
      max(col("salary")).as("max"),
      avg(col("salary")).as("avg")
    )

  metric2.show

  val json_struct = struct(
    col("group"),
    col("min"),
    col("max"),
    col("avg"),
  )


  val metric3 =metric2.withColumn("json",
    to_json(json_struct)
  )
    .select(col("json"))

  metric3.show(truncate = false)


  val struct_Json = StructType(Seq(
    StructField("group", StringType),
    StructField("min", DoubleType),
    StructField("max", DoubleType),
    StructField("avg", DoubleType),

  ))

  val metric4 = metric3.withColumn("from_Json",
    from_json(col("json"), struct_Json)
  )
    .select(
      col("from_Json.group").as("group"),
      col("from_Json.min").as("min"),
      col("from_Json.max").as("max"),
      col("from_Json.avg").as("avg"),
    )

  metric4.show(truncate = false)


  val metric5 = metric4.select(col("group"),
    expr("stack(3, 'min', min, 'max', max, 'avg', avg) as (param, val)")
  )


  metric5.show(truncate = false)
}
