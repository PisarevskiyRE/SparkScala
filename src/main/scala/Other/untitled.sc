import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

val spark = SparkSession.builder()
  .appName("TestSpark")
  .master("local")
  .getOrCreate()

val data = Seq(
  Row("Alice", 12),
  Row("Bo11123333333333333331b", 13)
)

val schema = Array(
  StructField("name", StringType, true),
  StructField("age", IntegerType, true),
)

val df = spark.createDataFrame(
  spark.sparkContext.parallelize(data),
  StructType(schema)
)

df.printSchema() // принтим схему
df.show


spark.stop()