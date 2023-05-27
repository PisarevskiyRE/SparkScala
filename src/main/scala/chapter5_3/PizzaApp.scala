package com.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{ Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Window}



object PizzaApp{

  /**
   * почему-то класс для преобразования датасета должен быть за пределами main
   */
  case class Orders(
                     _c0: Option[Int],
                     order_id: Option[Int],
                     date: Option[String],
                     user_id: Option[Int],
                     order_type: Option[String],
                     value: Option[Double],
                     address_id: Option[Int]
                   )

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)


    if (args.length != 2) {
      println("!!! -> Не указаны пути")
      System.exit(1)
    }

    val spark = SparkSession
      .builder()
      .appName("PizzaApp")
      .getOrCreate()


    val pizzaDF = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(args(0))


    import spark.implicits._
    val pizzaDS: Dataset[Orders] = pizzaDF.as[Orders]



    val ordersTotal = pizzaDS
      .groupBy("order_type")
      .count()
      .withColumnRenamed("count", "orders_total")


    val maxOrders = pizzaDS
      .groupBy("order_type", "address_id")
      .count()
      .withColumn("orders_cnt", col("count"))
      .withColumn("max_orders_cnt", max("count").over(Window.partitionBy("order_type")))
      .where(col("count") === col("max_orders_cnt"))
      .drop("max_orders_cnt", "count")


    val result = ordersTotal.join(maxOrders, Seq("order_type"), "inner")
      .select("order_type", "orders_total", "address_id", "orders_cnt")


    result.show()
    result.write.csv(args(1))

    /**
     *
     * ./spark/bin/spark-submit --class com.example.PizzaApp --deploy-mode client --master spark://4c60f4fe5f37:7077 --verbose --supervise /opt/spark-apps/SparkScala.jar /opt/spark-data/pizza_orders.csv /opt/spark-data/pizza_orders_result.csv
     *
     *
     * +----------+------------+----------+----------+
     * |order_type|orders_total|address_id|orders_cnt|
     * +----------+------------+----------+----------+
     * |       app|       89910|    409486|        18|
     * |      call|       10590|    405399|         6|
     * |      call|       10590|    399780|         6|
     * |      call|       10590|    404298|         6|
     * |      call|       10590|    401736|         6|
     * |       web|       12617|    398669|         6|
     * +----------+------------+----------+----------+
     */
  }
}




