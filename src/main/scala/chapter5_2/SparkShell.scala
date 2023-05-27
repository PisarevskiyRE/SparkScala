package chapter5_2

import org.apache.spark.sql.functions.{coalesce, length, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkShell extends App{


  val spark = SparkSession.builder()
    .appName("RDDs")
    .config("spark.master", "local[*]")
    .getOrCreate()


//  val sc = spark.sparkContext
//
//
//  val nums: Seq[Int] = (1 to 100000).toSeq
//
//  val numsRDD = sc.parallelize(nums)
//
//  println(numsRDD.getNumPartitions)
//  println(numsRDD.map(x => x).sum())


  val employeeDF: DataFrame = spark.read.option("header", "true").csv("usr/employee.csv")

  import spark.implicits._

  employeeDF.select($"Name", length($"Name").as("Length")).show
}



//   docker exec -it spark-cluster-spark-master-1 bash
//   ./spark/bin/spark-shell