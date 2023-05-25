package chapter4_3

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.time.{DayOfWeek, Instant, ZoneId}
import scala.io.Source


object LogData extends App {

  val spark = SparkSession.builder()
    .appName("RDDs")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  //818621,aio.jsc.nasa.gov,809893891,GET,/images/NASA-logosmall.gif,304,0

  case class Log(
                  id: Int,
                  host: String,
                  time: Int,
                  method: String,
                  url: String,
                  response: Int,
                  bytes: Int)


  def readLog(filename: String) =
    Source.fromFile(filename)
      .getLines()
      .drop(1)
      .map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))"))
      .map(values => Log(
        values(0).toInt,
        values(1),
        values(2).toInt,
        values(3),
        values(4),
        values(5).toInt,
        values(6).toInt)
      ).toList

  val logRDD: RDD[Log] = sc.parallelize(readLog("src/main/resources/logs_data.csv"))

  /**
   * все данные загружены
   */

  /*
    import spark.implicits._
    val logDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/logs_data.csv")
    val logDS = logDF.as[Log]
    val logRDD = logDS.rdd
    println(logRDD.count())
  */



  // 2. найти количество записей для каждого кода ответа (response)
  /**
   * тут вывелось так
   * Response 404: 3858
   * Response 302: 6912
   * Response 304: 258161
   * Response 200: 2696600
   * Response 501: 30
   *
   */
  val responseCount1: collection.Map[Int, Long] = logRDD.map(log => (log.response, 1)).countByKey

  responseCount1.foreach { case (response, count) =>
    println(s"Response $response: $count")
  }

  /**
   * похоже этот способ лучше, так как в выводе появились отдельные процессы на каждый ответ
   *
   * Response 501: 30
   * 23/05/25 17:05:25 INFO Executor: Finished task 9.0 in stage 4.0 (TID 51). 1321 bytes result sent to driver
   * 23/05/25 17:05:25 INFO TaskSetManager: Finished task 9.0 in stage 4.0 (TID 51) in 25 ms on host.docker.internal (executor driver) (9/12)
   * Response 302: 6912
   * 23/05/25 17:05:25 INFO Executor: Finished task 2.0 in stage 4.0 (TID 48). 1321 bytes result sent to driver
   * 23/05/25 17:05:25 INFO TaskSetManager: Finished task 2.0 in stage 4.0 (TID 48) in 154 ms on host.docker.internal (executor driver) (10/12)
   * Response 304: 258161
   * ...
   */
  val responseCount2: RDD[(Int, Int)] = logRDD.groupBy(_.response).mapValues(_.size)

  responseCount2.foreach { case (response, count) =>
    println(s"Response $response: $count")
  }

  // 3. следует собрать статистику по размеру ответа (bytes):
  // сколько всего байт было отправлено (сумма всех значений),
  // среднее значение, максимальное и минимальное значение.


  val totalBytes = logRDD.map(_.bytes).sum()
  val avgBytes = logRDD.map(_.bytes).mean()
  val maxBytes = logRDD.map(_.bytes).max()
  val minBytes = logRDD.map(_.bytes).min()


  println(s"totalBytes: $totalBytes")
  println(s"avgBytes: $avgBytes")
  println(s"maxBytes: $maxBytes")
  println(s"minBytes: $minBytes")

  /**
   * totalBytes: 4.3159977733E10
   * avgBytes: 14553.731227582262
   * maxBytes: 6823936
   * minBytes: 0
   */

  // 4. подсчитать количество уникальных хостов (host), для которых представлена статистика

  val uniqueHosts = logRDD.map(_.host).distinct().count()
  println(s"uniqueHosts: $uniqueHosts" )

  /**
   * uniqueHosts: 135268
   */

  // 5. найти топ-3 наиболее часто встречающихся хостов (host).
  // Результат анализа, выводимый на экран, должен включать в себя хоста
  // и подсчитанную частоту встречаемости этого хоста.

  val hostCounts = logRDD.map(log => (log.host, 1)).reduceByKey(_ + _)

  val topHosts = hostCounts.takeOrdered(3)(Ordering[Int].reverse.on(_._2))

  topHosts.foreach { case (host, count) =>
    println(s"TOP 3 ->  $host $count")
  }

  /**
   * TOP 3 ->  piweba3y.prodigy.com 19258
   * TOP 3 ->  piweba4y.prodigy.com 14512
   * TOP 3 ->  edams.ksc.nasa.gov 11472
   */

  // 6. определите дни недели (понедельник, вторник и тд), когда чаще всего выдавался ответ (response) 404.
  // В статистике отобразите день недели (в каком виде отображать день остается на ваше усмотрение, например, Mon, Tue)
  // и сколько раз был получен ответ 404. Достаточно отобразить топ-3 дней.

  val dayOfWeekCounts = logRDD
    .filter(_.response == 404)
    .map(log => {
      val instant = Instant.ofEpochSecond(log.time.toLong)
      val dayOfWeek = instant.atZone(ZoneId.systemDefault()).getDayOfWeek
      (dayOfWeek, 1)
    })
    .reduceByKey(_ + _)


  val topDaysOfWeek = dayOfWeekCounts.takeOrdered(3)(Ordering[Int].reverse.on(_._2))

  topDaysOfWeek.foreach { case (dayOfWeek, count) =>
    val dayOfWeekStr = dayOfWeek match {
      case DayOfWeek.MONDAY => "Понедельник"
      case DayOfWeek.TUESDAY => "Вторник"
      case DayOfWeek.WEDNESDAY => "Среда"
      case DayOfWeek.THURSDAY => "Четверг"
      case DayOfWeek.FRIDAY => "Пятница"
      case DayOfWeek.SATURDAY => "Суббота"
      case DayOfWeek.SUNDAY => "Воскресенье"
    }
    println(s"TOP 3 -> $dayOfWeekStr $count")
  }
  /**
   * TOP 3 -> Четверг 596
   * TOP 3 -> Суббота 593
   * TOP 3 -> Вторник 571
   */
}
