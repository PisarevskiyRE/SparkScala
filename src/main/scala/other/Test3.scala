package com.example
package other

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.Source

object Test3 extends App{

  case class Person(id: Int, name: String, age: Int)
  case class Salary(id: Int, month: String, salary: Double)



  def readPersonRawData(fileName: String) = {
    Source.fromFile(fileName)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(values => {
        Person(
          values(0).toInt,
          values(1),
          values(2).toInt
        )
      }).toList
  }

  def readSalaryRawData(fileName: String) = {
    Source.fromFile(fileName)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(values => {
        Salary(
          values(0).toInt,
          values(1),
          values(2).toDouble
        )
      }).toList

  }

  val spark = SparkSession
    .builder()
    .appName("TestRDD")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val sc = spark.sparkContext


  val personRDD = sc.parallelize(readPersonRawData("src/main/resources/person.csv"))
  val salaryRDD = sc.parallelize(readSalaryRawData("src/main/resources/salary.csv"))



//  personRDD.foreach(println)
//  salaryRDD.foreach(println)



  val avgSelaryPair = salaryRDD
    .map(salary => (salary.id, salary.salary))

  val avgSalaryRDD = avgSelaryPair
    .mapValues(salary => (salary, 1))
    .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
    .mapValues {
      case (totalSalary, count) => totalSalary / count
    }


  //avgSalaryRDD.foreach(println)

  val joinedRDD: RDD[(Int, ((Person, Double), Double))] = personRDD
    .map(person => (person.id, person))
    .join(avgSelaryPair)
    .join(avgSalaryRDD)


  val resultRDD = joinedRDD
    .filter { case (_, ((person, salary), avgSalary)) => salary > avgSalary }

  resultRDD.foreach(println)


  val ageGroup = Seq(
    (30, 35, "group1"),
    (35,40, "group2"),
    (40, 60, "group3")
  )


    personRDD.foreach(println)
    salaryRDD.foreach(println)




}
