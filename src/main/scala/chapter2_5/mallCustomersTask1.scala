package chapter2_5

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object mallCustomersTask1 {

  val spark = SparkSession
    .builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()
/*
CustomerID,
Gender,
Age,
Annual Income (k$),
Spending Score (1-100)
 */
  val mallCustomersSchema = StructType(Seq(
    StructField("CustomerID", IntegerType),
    StructField("Gender", StringType),
    StructField("Age", IntegerType),
    StructField("Annual Income (k$)", IntegerType),
    StructField("Spending Score (1-100)", IntegerType),
  ))


  val opt = Map(
    "inferSchema" -> "false",
    "header" -> "true",
    "path" -> "src/main/resources/mall_customers.csv",
  )


  val mallCustomers = spark.read
    .format("csv")
    .schema(mallCustomersSchema)
    .options(opt)
    .load()


  val mallCustomersWithAge: DataFrame = mallCustomers
    .withColumn("Age", col("Age")+2)

  val mallCustomersWithGroupAge = mallCustomersWithAge
    .withColumn("age_group", (col("Age") >= 30 && col("Age") < 36).cast(IntegerType))

  val mallCustomersWithGroupAgeFilter = mallCustomersWithGroupAge.filter(col("age_group") === 1)

  val incomeDF =  mallCustomersWithGroupAgeFilter
    .groupBy("Age","Gender")
    .agg(
      functions.round(functions.avg("Annual Income (k$)"),1).as("avg_income")
    )
    .orderBy("Gender", "Age")


   incomeDF
     .withColumn("gender_code", (col("Gender") === "Male" ).cast(IntegerType))
     .write
     .mode(SaveMode.Overwrite)
     .save("resources/data/customers")

  /*
    как то это все не очень выглядит, нельзя это бесконечными монадами писать?

    или какие-то подзапросы как в обычном sql?

 результат
 +---+------+----------+-----------+
  |Age|Gender|avg_income|gender_code|
  +---+------+----------+-----------+
  | 30|Female|      76.0|          0|
  | 31|Female|      72.5|          0|
  | 32|Female|      59.4|          0|
  | 33|Female|      51.7|          0|
  | 34|Female|      76.8|          0|
  | 35|Female|      86.0|          0|
  | 30|  Male|      88.3|          1|
  | 31|  Male|      28.0|          1|
  | 32|  Male|     118.0|          1|
  | 33|  Male|      25.0|          1|
  | 34|  Male|      99.6|          1|
  | 35|  Male|      77.5|          1|
  +---+------+----------+-----------+
   */
}
