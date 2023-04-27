package chapter3_4

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object HrDataset extends App{

  val spark = SparkSession
    .builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()

  import spark.implicits._


  val hrDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .csv("src/main/resources/hrdataset.csv")


  //подготавливаем для датасета
  val hrCleanDF = hrDF.select(
    $"PositionID",
    ltrim(rtrim(lower($"Position"))).as("Position")
  ).na.drop.distinct()



  case class hr(PositionID: Int, Position: String)

  //заворачиваем в типизированные строки
  val hrDS: Dataset[hr] = hrCleanDF.as[hr]

  //далее нужно удалить все знаки препинания
  def splitAndReplaceHrDS(ds: Dataset[hr]): Dataset[hr] = {
    ds.flatMap(pos => {
      val words = pos.Position.replaceAll("[^a-zA-Z\\s]", "").split("\\s+")
      words.map(w => hr(pos.PositionID, w))
    })
  }

  /* удаляем повторения
    не уверен можно ли это сделать через distinct(не вернется ли DataFrame???)
    на всякий случай напишу функцию
    но тоже кастыли
  */

  def hrDistinct(ds: Dataset[hr]): Dataset[hr] = {
    val distinctSet = ds.collect().toSet
    spark.createDataset(distinctSet.toSeq)
  }

  //поиск по DS, будем возвращать найденые позиции
  def hrsearch(request: List[String])(ds: Dataset[hr]):  Dataset[hr]= {
    ds.filter(
      pos => request.contains(pos.Position)
    )
  }

  val splitedAndReplacedHrDS: Dataset[hr] = splitAndReplaceHrDS(hrDS).distinct()
  val distinctSplitedAndReplacedHrDS: Dataset[hr] = hrDistinct(splitedAndReplacedHrDS)

  val request = List("BI", "it").map(r => r.toLowerCase.trim)

  val hrsearch: Dataset[hr] =  hrsearch(request)(distinctSplitedAndReplacedHrDS)


  val result: Dataset[(hr, hr)] = hrDS.joinWith(hrsearch,
    hrDS("PositionID") === hrsearch("PositionID")
  )

  // дальше как хотим, или дата сет тоже как то должен из себя селектить?
  result.select($"_1.PositionID", $"_1.Position").show

/*  Результат
 p.s.  сделал так что и в середине и в конце фразы ищется, из-за этого строк больше
 p.s. p.s. не соображаю как работать правильно с DS, с DF вроде бы худо бедно уяснисл
    +----------+--------------------+
    |PositionID|            Position|
    +----------+--------------------+
    |        13|  it manager - infra|
    |         5|         bi director|
    |        13|it manager - support|
    |        22| senior bi developer|
    |        12|         it director|
    |         4|        bi developer|
    |        14|          it support|
    |        13|     it manager - db|
    +----------+--------------------+
 */
}
