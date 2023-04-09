package Other

import org.apache.spark.sql.SparkSession


object PlayGround  extends App {

  val spark = SparkSession.builder()
    .appName("TestSpark")
    .master("local")
    .getOrCreate()

 /*
  или
  val spark = SparkSession.builder()
     .appName("Name of your Spark App")
     .config("spark.master", "local")
     .getOrCreate()
  */




  val courses = Seq(
    ("Scala", 22),
    ("Spark", 30)
  )
  
  import spark.implicits._
  val coursesDF = courses.toDF("title", "duration (h)")
  coursesDF.show()





  //toDF
  val data = Seq(
    ("Alice", 12),
    ("Bob", 13),
    ("Roman", 33)
  )
  val df = data.toDF("name", "age")
  df.show() // показываем данные
  df.printSchema() // принтим схему




  spark.stop()

}
