package chapter5_2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.time.{DayOfWeek, Instant, ZoneId}
import scala.io.Source
import scala.math.BigDecimal.double2bigDecimal

object Test extends App{


  val spark = SparkSession.builder()
    .appName("RDDs")
    .config("spark.master", "local[*]")
    .getOrCreate()


  val sc = spark.sparkContext


  val nums: Seq[Int] = (1 to 100000).toSeq

  val numsRDD = sc.parallelize(nums)

  println(numsRDD.getNumPartitions)
  println(numsRDD.map(x => x).sum())
}
