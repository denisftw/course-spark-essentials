package part3types

import java.sql.Date
import java.time.LocalDate

import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

object Datasets extends App {
  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()


  val numbersDF = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

  implicit val intEncoder = Encoders.scalaInt

  val numbersDS: Dataset[Int] = numbersDF.as[Int]
  numbersDS.map(_ + 1).show()

  // complex datasets
  // 1. define a case class
  case class Car(Name: String,
                 Miles_per_Gallon: Option[Double],
                 Cylinders: Long,
                 Displacement: Double,
                 Horsepower: Option[Long],
                 Weight_in_lbs: Long,
                 Acceleration: Double,
                 Year: LocalDate,
                 Origin: String)

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  // 2. read a data frame
  val carsDF = spark.read
    .schema(carsSchema)
    .json(s"src/main/resources/data/cars.json")

  // 3. define an encoder
//  implicit val carEncoder = Encoders.product[Car]
  import spark.implicits._

  // convert to DS
  val carsDS = carsDF.as[Car]

  val carNames = carsDS.map(_.Name)

  carsDS.filter(car => car.Horsepower.exists(_ > 200)
    && car.Year.getYear == 1973).show()

  /*
  1. count how many cars we have
  2. count how many powerful > 140
  3. compute the average horsepower for the whole DS
   */

  // 1. count how many cars we have
  println(carsDS.count())


  // 2. count how many powerful > 140
  println(carsDS.filter(
    _.Horsepower.exists(_ > 140)).count())

  // 3. compute the average horsepower for the whole DS
  import org.apache.spark.sql.functions._
  val carsWithHPDS = carsDS.flatMap(_.Horsepower.toSeq)
  carsWithHPDS
    .withColumnRenamed("value", "Horsepower")
    .agg(avg(col("Horsepower")))
    .show()

  val avgHP = carsWithHPDS.reduce(_ + _) * 1.0 / carsWithHPDS.count()
  println(avgHP)


  def readDF(filename: String): DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .json(s"src/main/resources/data/$filename")

  }
  case class Guitar(id: Long, make: String, model: String, `type`: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitars = readDF("guitars.json").as[Guitar]
  val guitarPlayers = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bands = readDF("bands.json").as[Band]

  val guitarPlayerBandDS = guitarPlayers.joinWith(bands,
    guitarPlayers.col("band") === bands.col("id"), "inner")

  guitarPlayerBandDS
    .withColumnRenamed("_1", "Player")
    .withColumnRenamed("_2", "Band")
    .show()


  /*
  1. join Player with Guitar so guitarId is contained in (array_contains)
   and outer join
   */
  val playerGuitars = guitars.joinWith(guitarPlayers,
    array_contains(guitarPlayers.col("guitars"), guitars.col("id")), "outer")

  playerGuitars.show()


  // Grouping
  val carsGroupedByOrigin = carsDS.groupByKey(_.Origin)
    .count().show()

}
