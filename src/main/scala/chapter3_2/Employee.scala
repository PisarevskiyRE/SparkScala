package chapter3_2

import org.apache.spark.sql.functions.{coalesce, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Employee {

  val spark = SparkSession
    .builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()


  val employeeDF: DataFrame = spark.read
    .option("header", "true")
    .csv("src/main/resources/employee.csv")



  import spark.implicits._

  // где null
  employeeDF
    .select("*")
    .where(
      $"birthday".isNull
    )
    .show


  // null первые
  employeeDF
    .select("*")
    .orderBy(
      $"birthday".desc_nulls_first
    )
    .show

  // удаление строчек где есть null
  employeeDF
    .na.drop()
    .show()


  // замена null
  employeeDF
    .na
    .fill("n/a", List("birthday", "date_of_birth"))
    .show

  // кастомная замена
  employeeDF
    .na.fill(
    Map(
      "birthday" -> "n/a",
      "date_of_birth" -> "Unknown"))
    .show

  // старый добрый калиск
  employeeDF
    .select(
      $"name",
      coalesce($"birthday", $"date_of_birth")
    )
    .show

  employeeDF
    .select(
      $"name",
      coalesce($"birthday", $"date_of_birth", lit("n/a"))
    )
    .show


  employeeDF.selectExpr(
    "name",
    "birthday",
    "date_of_birth",
    "ifnull(birthday, date_of_birth) as ifnull",
    "nvl(birthday, date_of_birth) as nvl",
    "nvl2(birthday, date_of_birth, 'Unknown') as nvl2"
  ).show


}
