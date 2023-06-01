package chapter6_4

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object UserScores extends App{
  import spark.implicits._

  val spark = SparkSession
    .builder()
    .appName("UserScores")
    .master("local[*]")
    .getOrCreate()

  // всегда как с большим объемом данных
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  val sc = spark.sparkContext

  val usersRDD = sc.textFile(s"src/main/resources/users.txt")
    .map{line =>
      val fields = line.split(" ")
      (fields(0).toLong, fields(1))
    }.partitionBy(new HashPartitioner(10))

  val scoresRDD = sc.textFile(s"src/main/resources/scores.txt")
    .map { line =>
      val fields = line.split(" ")
      (fields(0).toLong, fields(1).toDouble)
    }

  /**
   * 1 - плохое
   */
//  val joinedRDD: RDD[(Long, (Double, String))] = scoresRDD.join(usersRDD)
//
//  val resultRDD = joinedRDD
//    .reduceByKey((a, b) =>
//      if (a._1 > b._1) a else b)
//    .filter(_._2._1 >= 9.0)

  /**
   * 2
   */

//  val maxScoresRDD: RDD[(Long, Double)] = scoresRDD
//    .reduceByKey(Math.max)
//    .filter(_._2 >= 9.0)
//
//  val joinedRDD2: RDD[(Long, (Double, String))] = maxScoresRDD.join(usersRDD)

  /**
   * 3
   */

  val scoresPartitioner = {
    usersRDD.partitioner match {
      case None => new HashPartitioner(usersRDD.getNumPartitions)
      case Some(partitioner) => partitioner
    }
  }

  val repartitionedScoresRDD = scoresRDD.partitionBy(scoresPartitioner)

  val maxScoresRDD: RDD[(Long, Double)] = repartitionedScoresRDD
    .reduceByKey(Math.max)
    .filter(_._2 >= 9.0)

  val joinedRDD: RDD[(Long, (Double, String))] = maxScoresRDD.join(usersRDD)
}
