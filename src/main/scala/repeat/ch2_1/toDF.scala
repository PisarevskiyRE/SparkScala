package com.example
package repeat.ch2_1

import org.apache.parquet.format.IntType
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object toDF extends App{

  val spark = SparkSession.builder
    .appName("toDF")
    .master("local[*]")
    .getOrCreate()


  val data = Seq(
    Row("A",10),
    Row("B",10)
  )

  val schema: StructType = StructType(Array(
    StructField("Name", StringType, true),
    StructField("Age", IntegerType, true)
  ))

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(data),
    schema
  )


  df.show()
  df.printSchema()



}
