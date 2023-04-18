package chapter2_5

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object WinterIsComing {

  val spark = SparkSession
    .builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()


 val season1: DataFrame = spark.read
    .option("inferSchema", "true")
    .csv("src/main/resources/subtitles_s1.json")

 val season2: DataFrame = spark.read
    .option("inferSchema", "true")
    .csv("src/main/resources/subtitles_s2.json")

  import spark.implicits._
  import spark.sqlContext.implicits._

  // читаю везде пишут должно работать $"_c0" вместо col("_c0"), не знаю в чем дело у меня на $ горит и ничего не может вывести

  val season1DS =
      season1.select(explode(split(col("_c0"), "\\W+")).as("w_s1"))
        .filter(col("w_s1") =!= "")
        .groupBy(col("w_s1"))
        .agg(
          functions.count("w_s1").as("cnt_s1")
        )
        .orderBy(desc("cnt_s1"))
        .withColumn(
          "id1"
          ,row_number.over(Window.orderBy(desc("cnt_s1")))  - 1
        )
        .limit(20)


  val season2DS =
    season2.select(explode(split(col("_c0"), "\\W+")).as("w_s2"))
      .filter(col("w_s2") =!= "")
      .groupBy(col("w_s2"))
      .agg(
        functions.count("w_s2").as("cnt_s2")
      )
      .orderBy(desc("cnt_s2"))
      .withColumn(
        "id2"
        , row_number.over(Window.orderBy(desc("cnt_s2"))) - 1
      )
      .limit(20)

  val joinCondition = season1DS.col("id1") === season2DS.col("id2")

  val seasonыJoinDF = season1DS.join(season2DS, joinCondition, "inner")


  seasonыJoinDF.select(col("w_s1"), col("cnt_s1"),col("id1").as("id"), col("w_s2"), col("cnt_s2"))
  //.show
    .write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .save("resources/data/wordcount")

  /*
  результат
  +----+------+---+----+------+
  |w_s1|cnt_s1| id|w_s2|cnt_s2|
  +----+------+---+----+------+
  | the|  1481|  0|   I|  1489|
  |   I|  1349|  1| the|  1381|
  | you|  1128|  2| you|  1268|
  |  to|  1030|  3|  to|  1052|
  |   a|   777|  4|   a|   795|
  |   s|   668|  5|   s|   624|
  |  of|   625|  6|  of|   611|
  | and|   554|  7| You|   546|
  | You|   478|  8|   t|   513|
  |   t|   414|  9| and|   488|
  |  me|   405| 10|  me|   433|
  |your|   389| 11|your|   409|
  |  is|   386| 12|  in|   382|
  |  it|   358| 13|  is|   375|
  | for|   334| 14|  it|   369|
  |  in|   331| 15| for|   353|
  |  my|   330| 16|  my|   322|
  | The|   302| 17|have|   306|
  |that|   299| 18| The|   298|
  |  be|   288| 19| And|   297|
  +----+------+---+----+------+
   */
}
