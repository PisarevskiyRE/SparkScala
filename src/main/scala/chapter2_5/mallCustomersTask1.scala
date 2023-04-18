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

  val isMale: Column = col("gender") === "Male"
  val hasProperAge: Column = col("age").between(30, 35)
  val trueAge = col("Age") + 2


  val mallCustomersWithAgeDF: DataFrame = mallCustomers
    .withColumn("Age", trueAge)

  val mallCustomersWithGroupAgeDF: DataFrame = mallCustomersWithAgeDF
    .withColumn("age_group", (hasProperAge).cast(IntegerType))

  val mallCustomersWithGroupAgeFilterDF = mallCustomersWithGroupAgeDF.filter(col("age_group") === 1)

  val incomeDF =  mallCustomersWithGroupAgeFilterDF
    .groupBy("Age","Gender")
    .agg(
      round(avg("Annual Income (k$)"),1).as("avg_income")
    )
    .orderBy("Gender", "Age")

  val incomeWithCodeDF = incomeDF
    .withColumn(
      "gender_code",
      isMale.cast(IntegerType)
    )

  incomeWithCodeDF
     .write
     .mode(SaveMode.Overwrite)
     .option("header","true")
     .save("resources/data/customers")
/*

^- как делать не надо


как надо делать ->

def read(
      opt: Map[String, String],
      format: String,
      schema: StructType)
    : DataFrame = {

    spark.read
      .format(format)
      .options(opt)
      .schema(schema)
      .load()
  }

  def withProperAge(df: DataFrame): DataFrame =
    df.withColumn("Age", col("Age").plus(2))


  def withGenderCode(df: DataFrame): DataFrame = {
    val isFemale = col("Gender") === "Female"
    val isMale = col("Gender") === "Male"

    df.withColumn("gender_code",
      when(isMale, 1)
        .when(isFemale, 0)
        .otherwise(-1))
  }

  def extractCustomerGroups(df: DataFrame): DataFrame = {
    val columns = Seq(col("Gender"), col("Age"))
    val hasProperAge = col("Age").between(30, 35)

    df
      .filter(hasProperAge)
      .groupBy(columns: _*)
      .agg(round(
        avg("Annual Income (k$)"),
        1).as("avg_income"))
      .orderBy(columns: _*)
  }


  val customersDF = read(opt, format, schema)

  val incomeDF = customersDF
    .transform(withProperAge)
    .transform(extractCustomerGroups)
    .transform(withGenderCode)
   */
}
