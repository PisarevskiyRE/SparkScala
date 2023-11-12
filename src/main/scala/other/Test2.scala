package com.example
package other

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, sum}
import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, SparkSession, TypedColumn}
import shapeless.test.typed




object Test2 extends App {

  case class Person(id: Int, name: String, age: Int)

  case class Salary(id: Int, month: String, salary: Double)

  val spark = SparkSession
    .builder()
    .appName("Test2")
    .master("local")
    .getOrCreate()


  val personDF = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .csv("src/main/resources/person.csv")

  val salaryDF = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .csv("src/main/resources/salary.csv")

  import spark.implicits._

  val personDS: Dataset[Person] = personDF.as[Person]
  val salaryDS: Dataset[Salary] = salaryDF.as[Salary]


  //2. Вывести месяца и информацию по сотруднику когда его зарплата была выше средней для сотрудника


  val joinCOndition = col("a.id") === col("b.id")

  val joinedDS = personDS.as("a").joinWith(salaryDS.as("b"), joinCOndition, "inner")

  case class PersonWithAVG(personId: Int, salary: Double, month: String, avg: Double)


  val newStruct: Dataset[PersonWithAVG] = joinedDS
    .map(x=>  PersonWithAVG(x._1.id, x._2.salary, x._2.month, 0))



  newStruct.show()












}
