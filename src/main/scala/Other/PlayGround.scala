package Other

import org.apache.spark.sql.SparkSession


object PlayGround extends App {

  val spark = SparkSession.builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()

  val courses = Seq(
    ("Scala", 22),
    ("Spark", 30)
  )
  
  import spark.implicits._
  
  val coursesDF = courses.toDF("title", "duration (h)")
  
  coursesDF.show()
  spark.stop()

}
