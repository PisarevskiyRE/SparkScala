package chapter6_1

import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object PhysicalPlan extends App{

  val spark = SparkSession
    .builder()
    .appName("PhysicalPlan")
    .master("local")
    .getOrCreate()


  val physicalPlanDF: DataFrame =
    spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("src/main/scala/chapter6_1/employee.csv")


  import spark.implicits._

  val salaryByDepartmentDF = physicalPlanDF
    .groupBy($"department")
    .agg(avg($"salary").as("avg"))


  salaryByDepartmentDF.explain()

  //result.explain()
  //result.explain(mode="extended")
  //result.explain(mode="cost")
  //result.explain(mode="formatted")
  //result.explain(mode="codegen")
}
