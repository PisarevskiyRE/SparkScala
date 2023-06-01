package chapter6_3

import org.apache.spark.sql.functions.{asc, col, desc, lit, reverse, upper}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Partition extends App {

  val spark = SparkSession
    .builder()
    .appName("PartitionApp")
    .master("local[*]")
    .getOrCreate()

  // всегда как с большим объемом данных
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  def addColumn(df: DataFrame, n: Int): DataFrame = {
    val columns = (1 to n ).map(col => s"col_$col")
    columns.foldLeft(df)((df, column) => df.withColumn(column, lit("n/a")))
  }

  val data1 = (1 to 500000).map(i => (i, i*100))
  val data2 = (1 to 10000).map(i => (i, i*1000))

  import spark.implicits._

  val df1 = data1.toDF("id","salary").repartition(5)
  val df2 = data2.toDF("id","salary").repartition(10)

  /**
   * Способ 1
   */

//  val dfWithColumns = addColumn(df2, 10)
//  val joinedDF1 = dfWithColumns.join(df1, "id")
//
//  joinedDF1.show()

  /**
   * Способ 2
   */
  val repartitionedById1 = df1.repartition(col("id"))
  val repartitionedById2 = df2.repartition(col("id"))

  val joinedDF2 = repartitionedById2.join(repartitionedById1, "id")
  val dfWithColumns2 = addColumn(joinedDF2, 10)

  /**
   * Способ 2 измененный
   */
  def toUpperCase(colName: String)(df: DataFrame): DataFrame = {
    df.withColumn(colName, upper(col(colName)));
  }

  def reverseCol(colName: String)(df: DataFrame): DataFrame = {
    df.withColumn(colName, reverse(col(colName)));
  }

  //
  def sumCols(colName1: String, colName2: String, colName3: String)(df: DataFrame): DataFrame = {
    df.withColumn(colName1, (col(colName2).cast("Int") + col(colName3).cast("Int")));
  }

  val repartitionedById3 = df1.repartition(col("id"))
  val repartitionedById4 = df2.repartition(col("id"))


  val  dfWithColumns3 = addColumn(repartitionedById3, 10)

  val dfWithTransfomedSortColumns =
    dfWithColumns3
      .transform(reverseCol("col_5"))
      .transform(reverseCol("col_1"))
      .transform(reverseCol("col_2"))
      .transform(toUpperCase("col_1"))
      .transform(toUpperCase("col_3"))
      .transform(toUpperCase("col_5"))
      .transform(sumCols("col_4", "id","salary"))
      .orderBy(desc("col_3"),desc("id"),asc("salary"))


  val joinedDF3 = dfWithTransfomedSortColumns.join(repartitionedById4, "id")


  dfWithColumns2.show(200) //2
  joinedDF3.show(200) // *2

  dfWithColumns2.explain() // 2
  joinedDF3.explain() // *2


  System.in.read()
}

/**
 * 1. Вот хитрый catalyst... я пока не посмотрел план не мог понять почему
 * в простых трансформация не изменяется план...
 */
