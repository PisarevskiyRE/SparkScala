package chapter3_3

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object AiJobsIndustry extends App {

  val spark = SparkSession
    .builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()


  val AiJobsIndustryDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("multiLine", "true")
    .option("lineSep", "\n")
    .option("ignoreLeadingWhiteSpace", "true")
    .option("ignoreTrailingWhiteSpace", "true")
    .option("escape", "\"")
    .csv("src/main/resources/AiJobsIndustry.csv")


  import spark.implicits._

  val AiJobsIndustryCleanDF = AiJobsIndustryDF
    .select(
      lower(ltrim(rtrim($"JobTitle"))).as("JobTitle"),
      lower(ltrim(rtrim(regexp_replace($"Company", "\n", "")))).as("Company"),
      lower(ltrim(rtrim($"Location"))).as("Location"),
      lower(ltrim(rtrim(
        regexp_replace($"CompanyReviews", "[^0-9]", "").cast("integer")
      ))).as("CompanyReviews"),
      lower(ltrim(rtrim(regexp_extract($"link", "fccid=([^']+)'", 1)))).as("link")
      // линк получается можно в ключ работы включить, грубо говоря
    )
    .na.drop(List("CompanyReviews")) // считаем оценки, главное поле
    .na.fill(Map(
      "JobTitle"->"n/a",
      "Company"->"n/a",
      "Location"->"n/a"))
    .where(
      (($"JobTitle" =!= "n/a") && ($"Company" =!= "n/a") && ($"Location" =!= "n/a")) or
        (($"JobTitle" =!= "n/a") && ($"Company" =!= "n/a"))
      // не знаю какие требования к данным тут много еще всяких проверок поставтить на "чушь"
          )
    .distinct() // сомнительно, но пока так


  // после чистки всего на мой взгляд сомнительного осталось всего 4 записи без link
  // из чего я могу предположить что на линк правильнее ссылаться
  // проще всего будет обьеденить эти поля

/*
    AiJobsIndustryCleanDF.select("*")
      .filter(length($"link") < 2 )
      .show(500)
*/

  // готовим теперь два дата фрейма для работ и компаний
  val JobsDF = AiJobsIndustryCleanDF
    .select(
      coalesce($"link",$"JobTitle").as("JobId"),
      $"JobTitle",
      $"Location",
      $"CompanyReviews".cast("integer")
    ).na.drop(List("JobId"))

  val jobTotalDF = JobsDF
    .groupBy("JobId")
    .agg(
      sum("CompanyReviews").as("TotalCompanyReviews")
    )

  val job2MaxMinDF = jobTotalDF
    .select(
      $"JobId",
       $"TotalCompanyReviews",

    )
    .withColumn("MaxTotalCompanyReviews", max($"TotalCompanyReviews").over())
    .withColumn("MinTotalCompanyReviews", min($"TotalCompanyReviews").over())
    .withColumn("Type", // case when :)
        when($"TotalCompanyReviews" === $"MaxTotalCompanyReviews", "Max" )
      .when($"TotalCompanyReviews" === $"MinTotalCompanyReviews", "Min" ))
    .filter($"TotalCompanyReviews" === $"MaxTotalCompanyReviews" || $"TotalCompanyReviews" === $"MinTotalCompanyReviews")


  val JobResult = JobsDF.
    select(
      $"JobId",
      $"JobTitle",
      $"Location",
      $"CompanyReviews"
    ).as("t1")
    .join(job2MaxMinDF.as("t2"), $"t1.JobId" === $"t2.JobId")
    .groupBy("Type")
    .agg(
        concat_ws(", ", collect_set("JobTitle")).as("JobTitles"),
        concat_ws(", ", collect_set("Location")).as("Locations"),
        avg($"TotalCompanyReviews") // не знаю как протащить поле без группировки
      )


  val companyDF = AiJobsIndustryCleanDF
    .select(
      $"Company",
      $"Location",
      $"CompanyReviews".cast("integer")
    ).na.drop(List("Company"))

  val companyTotalDF  = companyDF
    .groupBy("Company")
    .agg(
      sum("CompanyReviews").as("TotalCompanyReviews")
    )

  val companyMaxMinDF = companyTotalDF
    .select(
      $"Company",
      $"TotalCompanyReviews",
    )
    .withColumn("MaxTotalCompanyReviews", max($"TotalCompanyReviews").over())
    .withColumn("MinTotalCompanyReviews", min($"TotalCompanyReviews").over())
    .withColumn("Type", // case when :)
      when($"TotalCompanyReviews" === $"MaxTotalCompanyReviews", "Max")
        .when($"TotalCompanyReviews" === $"MinTotalCompanyReviews", "Min"))
    .filter($"TotalCompanyReviews" === $"MaxTotalCompanyReviews" || $"TotalCompanyReviews" === $"MinTotalCompanyReviews")


  val companyResult = companyDF.
    select(
      $"Company",
      $"Location",
      $"CompanyReviews"
    ).as("t1")
    .join(companyMaxMinDF.as("t2"), $"t1.Company" === $"t2.Company")
    .groupBy("Type")
    .agg(
      concat_ws(", ", collect_set("t1.Company")).as("Titles"),
      concat_ws(", ", collect_set("Location")).as("Locations"),
      avg($"TotalCompanyReviews") // не знаю как протащить поле без группировки
    )

  companyResult.union(JobResult).show



  case class Job(
     JobId: String,
     JobTitle: String,
     Location: String,
     CompanyReviews: Int
   )

  case class Company(
      Company: String,
       Location: String,
       CompanyReviews: Int
 )


  val jobDS: Dataset[Job] = JobsDF.as[Job]
  val companyDS = companyDF.as[Company]

  val jobTotalDS = jobDS
    .groupBy("JobId")
    .agg(
      sum("CompanyReviews").as("TotalCompanyReviews")
    )

  val jobMaxMinDS = jobTotalDS
    .select(
      $"JobId",
      $"TotalCompanyReviews",

    )
    .withColumn("MaxTotalCompanyReviews", max($"TotalCompanyReviews").over())
    .withColumn("MinTotalCompanyReviews", min($"TotalCompanyReviews").over())
    .withColumn("Type", // case when :)
      when($"TotalCompanyReviews" === $"MaxTotalCompanyReviews", "Max")
        .when($"TotalCompanyReviews" === $"MinTotalCompanyReviews", "Min"))
    .filter($"TotalCompanyReviews" === $"MaxTotalCompanyReviews" || $"TotalCompanyReviews" === $"MinTotalCompanyReviews")


  val JobResultDS = jobDS.
    select(
      $"JobId",
      $"JobTitle",
      $"Location",
      $"CompanyReviews"
    ).as("t1")
    .join(jobMaxMinDS.as("t2"), $"t1.JobId" === $"t2.JobId")
    .groupBy("Type")
    .agg(
      concat_ws(", ", collect_set("JobTitle")).as("JobTitles"),
      concat_ws(", ", collect_set("Location")).as("Locations"),
      avg($"TotalCompanyReviews") // не знаю как протащить поле без группировки
    )



  val companyTotalDS = companyDS
    .groupBy("Company")
    .agg(
      sum("CompanyReviews").as("TotalCompanyReviews")
    )

  val companyMaxMinDS = companyTotalDS
    .select(
      $"Company",
      $"TotalCompanyReviews",
    )
    .withColumn("MaxTotalCompanyReviews", max($"TotalCompanyReviews").over())
    .withColumn("MinTotalCompanyReviews", min($"TotalCompanyReviews").over())
    .withColumn("Type", // case when :)
      when($"TotalCompanyReviews" === $"MaxTotalCompanyReviews", "Max")
        .when($"TotalCompanyReviews" === $"MinTotalCompanyReviews", "Min"))
    .filter($"TotalCompanyReviews" === $"MaxTotalCompanyReviews" || $"TotalCompanyReviews" === $"MinTotalCompanyReviews")


  val companyResultDS = companyDS.
    select(
      $"Company",
      $"Location",
      $"CompanyReviews"
    ).as("t1")
    .join(companyMaxMinDS.as("t2"), $"t1.Company" === $"t2.Company")
    .groupBy("Type")
    .agg(
      concat_ws(", ", collect_set("t1.Company")).as("Titles"),
      concat_ws(", ", collect_set("Location")).as("Locations"),
      avg($"TotalCompanyReviews")
    )

  companyResultDS.union(JobResultDS).show
  companyResult.union(JobResult).show



  /*

  В обычном sql такой запрос ну минут 10 занял написать
  Тут конечно у меня "подзавихрились" извилины, очень сложно начинать думать по другому.
  А еще же надо "правильно" перевести все это на функциональные рельсы с map`ами flatmap`ами и тд...
    вообще не представляю как можно сделать это быстро for comprehension не практикуется?
  По суте сейчас просто df типизирован
  Я буду думать еще как это все можно реализвовать функционально
  p.s. задание прям супер сложное с непривычки :)
  p.s. p.s. а если еще учесть что нужно все хорошо оформить, красиво,
    с выделением функций, забиванием стандартных полей, повеситься можно))


  Пока что результат таков

  +----+--------------------+--------------------+------------------------+
  |Type|              Titles|           Locations|avg(TotalCompanyReviews)|
  +----+--------------------+--------------------+------------------------+
  | Min|reprisk, nascent ...|wellington city, ...|                     2.0|
  | Max|          amazon.com|calle blancos, pr...|               5507037.0|
  | Min|java engineer, co...|wellington city, ...|                     2.0|
  | Max|payroll operation...|edinburgh, calle ...|               6182991.0|
  +----+--------------------+--------------------+------------------------+
   */
}
