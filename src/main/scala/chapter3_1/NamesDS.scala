package chapter3_1

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object NamesDS {

  val spark = SparkSession
    .builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()

  val namesDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/names.csv")

  namesDF.printSchema()

  //implicit val stringEncoder = Encoders.STRING
  import spark.implicits._

  val namesDS: Dataset[String] = namesDF.as[String]

  namesDS.filter(x => x != "Bob")




}
