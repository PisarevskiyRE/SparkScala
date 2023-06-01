package chapter6_4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object BroadcastJoin extends App{

  val spark = SparkSession
    .builder()
    .appName("BroadcastJoin")
    .master("local[*]")
    .getOrCreate()

  // всегда как с большим объемом данных
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


  val sc = spark.sparkContext

  import spark.implicits._

  val smallDF = sc.parallelize(Seq(
    (1, "a1"),
    (2, "b2"),
    (2, "c3"),
  )).toDF("id", "code")

  val bigDS = spark.range(1, 100000000)

  val joinedDF = bigDS.join(broadcast(smallDF), "id")
  joinedDF.explain()

  joinedDF.show(200)

  System.in.read()
}


