package com.example
package repeat.ch2_5

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object Subs extends App {

  val spark = SparkSession.builder()
    .appName("Subs")
    .master("local")
    .getOrCreate()


  val s1 = spark.read
    .option("inferSchema", "true")
    .csv("src/main/resources/subtitles_s1.json")


  val s2 = spark.read
    .option("inferSchema", "true")
    .csv("src/main/resources/subtitles_s2.json")


  val s1Exp = s1.select(
    explode(split( col("_c0"), "\\W+")).as("words")
  )
    .select(
      upper(col("words")).as("word")
    )
    .where(col("word") =!= "")
    .groupBy("word")
    .agg(
      count(col("word")).as("count")
    )
    .withColumn("id1",
      row_number().over(Window.orderBy(col("count").desc))
    )
    .limit(10)


  val s2Exp = s2.select(
      explode(split(col("_c0"), "\\W+")).as("words2")
    )
    .select(
      upper(col("words2")).as("word2")
    )
    .where(col("word2") =!= "")
    .groupBy("word2")
    .agg(
      count(col("word2")).as("count2")
    )
    .withColumn("id2",
      row_number().over(Window.orderBy(col("count2").desc))
    )
    .limit(10)



  val joinCondition = col("id1") === col("id2")

  val joinedDf = s1Exp.join(s2Exp, joinCondition, "inner")





  joinedDf.show(truncate = false)


  joinedDf.write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .save("src/main/resources/data/wordcount")

}
