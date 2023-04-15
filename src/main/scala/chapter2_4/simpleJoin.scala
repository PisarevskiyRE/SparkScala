package chapter2_4

import org.apache.spark.sql.SparkSession

object simpleJoin {

  val spark = SparkSession
    .builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()

  val valuesDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/stack_link_value.csv")


  val tagsDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/stack_links.csv")

  //условие соединения
  val joinCondition = valuesDF.col("id") === tagsDF.col("key")




  val innerJoinDF = tagsDF.join(valuesDF, joinCondition, "inner")

  val fullOuterDF = tagsDF.join(valuesDF, joinCondition, "outer")

  val leftOuterDF = tagsDF.join(valuesDF, joinCondition, "left_outer")

  val rightOuterDF = tagsDF.join(valuesDF, joinCondition, "right_outer")

  val leftSemiDF = tagsDF.join(valuesDF, joinCondition, "left_semi")

}
