package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataFramesBasics extends App {

  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  val cpuTuples = Seq(
    ("Pentium 100",100,1994,"Intel"),
    ("Celeron 300A",300,1998,"Intel"),
    ("Duron 700",700,2000,"AMD"),
    ("Athlon 1700+",1500,2002,"AMD"),
  )

  import spark.implicits._
  val cpuDF = cpuTuples.toDF("Name", "Frequency", "Year", "Manufacturer")
  cpuDF.show()


  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  moviesDF.printSchema()

  println(moviesDF.count())
}
