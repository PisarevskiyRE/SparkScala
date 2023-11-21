package com.example
package repeat.other

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}

import scala.util.Random

object Test5 extends App {

  case class Person(id: Int, name: String, Age: Int)
  case class Salary(id: Int, month: Int, salary: Double)


  val persons: Seq[Person] = (1 to 100).map( x =>
    Person(x, "Name"+x.toString, Random.nextInt(15)+30)
  ).toSeq


  val salarys: Seq[Salary] = (1 to 12).flatMap(x => {
    persons.map(y =>
      Salary(y.id, x, Random.nextInt(500) + 1000)
    )
  }
  ).toSeq

//  persons.foreach(println)
//  salarys.foreach(println)


  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Test5")
    .getOrCreate()


  import spark.implicits._

  val personDF = persons.toDF().as("persons")
  val salaryDF = salarys.toDF().as("salarys")


//  personDF.show
//  salaryDF.show

  val joinCondition = col("persons.id") === col("salarys.id")



  val joinedDF = personDF.join(salaryDF,joinCondition,"inner")


  val metric1 = joinedDF
    .withColumn(
      "avgPerPerson",
      avg("salarys.salary").over(Window.partitionBy("persons.id") )
    )
    .where(
      col("salarys.salary") > col("avgPerPerson")
    )
    .select("persons.name", "salarys.month", "salarys.salary", "avgPerPerson")

  //metric1.show()


  val groups = Seq(
    (30, 35, "a"),
    (35,40,"b"),
    (40,100,"c")
  )


  val metric2 = joinedDF
    .withColumn("groupPerson",
      when(col("persons.age").between(groups(0)._1, groups(0)._2) , groups(0)._3)
        .when(col("persons.age").between(groups(1)._1, groups(1)._2) , groups(1)._3)
        .when(col("persons.age").between(groups(2)._1, groups(2)._2) , groups(2)._3)
    )
    .groupBy("groupPerson")
    .agg(
      min(col("salarys.salary")).as("min"),
      max(col("salarys.salary")).as("max"),
      avg(col("salarys.salary")).as("avg"),
    )

  //metric2.show



  val json_struct =  struct(
    col("groupPerson"),
    col("min"),
    col("max"),
    col("avg")
  )

  val metric3 = metric2
    .withColumn("json", to_json(json_struct))

  //metric3.show


  val json = new StructType()
    .add("groupPerson", StringType)
    .add("min", DoubleType)
    .add("max", DoubleType)
    .add("avg", DoubleType)


  val metric4 = metric3
    .withColumn( "unpvt", from_json(col("json"), json))
    .select(
      col("unpvt.groupPerson").as("groupPerson"),
      col("unpvt.min").as("min"),
      col("unpvt.max").as("max"),
      col("unpvt.avg").as("avg")
    )







  metric4.show
 // pivotedDF.show

}
