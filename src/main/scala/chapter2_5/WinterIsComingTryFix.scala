package chapter2_5

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}

object WinterIsComingTryFix {

  val spark = SparkSession
    .builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()


  //[1] извлечение
  def read(opt: Map[String, String], path: String): DataFrame = {
    spark.read
      .options(opt)
      .csv(path) // тут можно похалтурить пока
  }

  val readOptions = Map("inferSchema" -> "true")


  val season1DF = read(readOptions, "src/main/resources/subtitles_s1.json")
  val season2DF = read(readOptions, "src/main/resources/subtitles_s2.json")


  //[2] трансформация (с выделением функций)
  // 1 split
  // 2 filter
  // 3 lowerCase
  // 4 groupBy
  // 4+ count
  // 5 sort
  // 6 identity
  // 7 limit

  import spark.implicits._


  //магия колонок
  object DfColumn extends DfColumn{
    implicit def columnToString(col: DfColumn.Value): String = col.toString
  }

  trait DfColumn extends Enumeration{
    val id,
    w_s1, w_s2,
    cnt_s1, cnt_s2 = Value
  }


  def splitData(colName: String)(df: DataFrame): DataFrame = {
    df.select(explode(split($"_c0", "\\W+")).as(colName))
  }

  def toLowerCase(colName: String)(df: DataFrame): DataFrame = {
   df.withColumn(colName, lower(col(colName)));
  }

  def filterEmptyData(colName: String)(df: DataFrame): DataFrame = {
    df.filter(col(colName) =!= "")
  }

  def groupForNumData(colName: String)(colCountName: String)(df: DataFrame): DataFrame = {
    df.groupBy(col(colName))
      .agg(
        count(col(colName)).as(colCountName)
      )
  }

  def orderingTopData(sortExpr: Column)(df: DataFrame): DataFrame ={
    df.orderBy(sortExpr)
  }

  def addColId(colName: String)(sortExpr: Column)(df: DataFrame): DataFrame = {
    df.withColumn(
      colName
      ,row_number.over(Window.orderBy(sortExpr)) - 1
    )
  }

  def getTopRows(cnt: Int)(df: DataFrame): DataFrame = {
    df .limit(cnt)
  }

  val season1DS = season1DF
    .transform(splitData(DfColumn.w_s1))
    .transform(filterEmptyData(DfColumn.w_s1))
    .transform(toLowerCase(DfColumn.w_s1))
    .transform(groupForNumData(DfColumn.w_s1)(DfColumn.cnt_s1))
    .transform(orderingTopData(desc(DfColumn.cnt_s1)))
    .transform(addColId(DfColumn.id)(desc(DfColumn.cnt_s1)))
    .transform(getTopRows(20))

  val season2DS = season2DF
    .transform(splitData(DfColumn.w_s2))
    .transform(filterEmptyData(DfColumn.w_s2))
    .transform(toLowerCase(DfColumn.w_s2))
    .transform(groupForNumData(DfColumn.w_s2)(DfColumn.cnt_s2))
    .transform(orderingTopData(desc(DfColumn.cnt_s2)))
    .transform(addColId(DfColumn.id)(desc(DfColumn.cnt_s2)))
    .transform(getTopRows(20))


  // просто так не получилось вывести с алиасами заработало
  // такая ошибка была
  // Reference 'id' is ambiguous, could be: id, id.

  val joinCondition = season1DS.col(DfColumn.id) === season2DS.col(DfColumn.id)

  val seasonsJoinDF = season1DS.as("s1").join(season2DS.as("s2"), joinCondition, "inner")

  val finalDF = seasonsJoinDF.select(
    col("w_s1"),
    col("cnt_s1"),
    col("s1.id"),
    col("w_s2"),
    col("cnt_s2"))



  //[3] сохрание
  finalDF.write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .save("resources/data/wordcount")

  /*
  постарался учесть все замечания
  результат
  +----+------+---+----+------+
  |w_s1|cnt_s1| id|w_s2|cnt_s2|
  +----+------+---+----+------+
  | the|  1783|  0| you|  1814|
  | you|  1606|  1| the|  1679|
  |   i|  1349|  2|   i|  1489|
  |  to|  1048|  3|  to|  1074|
  |   a|   886|  4|   a|   908|
  | and|   779|  5| and|   785|
  |   s|   668|  6|  of|   649|
  |  of|   647|  7|   s|   624|
  |  it|   547|  8|  it|   527|
  |your|   476|  9|your|   517|
  |  he|   469| 10|   t|   513|
  |  my|   458| 11|  me|   438|
  |  is|   437| 12|  my|   432|
  |   t|   414| 13|  is|   402|
  |  me|   407| 14|  he|   402|
  |that|   374| 15|  in|   401|
  |  in|   354| 16|that|   389|
  | for|   349| 17| for|   377|
  |what|   338| 18|  we|   348|
  |  be|   292| 19|what|   335|
  +----+------+---+----+------+
*/
}
