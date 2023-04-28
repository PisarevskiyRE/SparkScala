package chapter4_1

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object partition extends App{


  val ids = List.fill(5)("id-").zipWithIndex.map(x => x._1 + x._2)

  val spark = SparkSession
    .builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  val idsDS: Dataset[String] = ids.toDF.as[String]

  val idsPartitioned = idsDS.repartition(6)

  val numPartitions = idsPartitioned.rdd.partitions.length
  println(s"partitions = ${numPartitions}")


  idsPartitioned.rdd
    .mapPartitionsWithIndex(
      (partition: Int, it: Iterator[String]) =>
        it.toList.map(id => {
          println(s" partition = $partition; id = $id")
        }).iterator
    ).collect

}
