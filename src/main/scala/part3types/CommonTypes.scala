package part3types

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.FloatType

object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()


  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  import org.apache.spark.sql.functions._
  moviesDF.select(col("Title"), lit(47).as("plain_value"))
//    .show()

  // Boolean
  val dramaFilter = col("Major_genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter
  moviesDF.select("Title").where(preferredFilter)

  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))
  moviesWithGoodnessFlagsDF.where("good_movie")
//    .show()

  // negations
  moviesWithGoodnessFlagsDF.where(not(col("good_movie")))

  val moviesAvgRatingsDF = moviesDF.select(col("Title"),
    (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating"))/2)

  // correlation
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating"))

  // strings
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDF.select(initcap(col("Name"))).show()

  // contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))

  // regex
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "")

  // regex replacement

  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  )//.show()

  // 1. filter the cars DF by a  list of car names
  def getCarNames: List[String] = List("plymouth satellite", "ford torino")

  val carNameFilter = getCarNames.foldLeft(lit(false)) { (cond, name) =>
    cond or (col("Name").contains(name))
  }

  val carNameRegexFilter = regexp_extract(col("Name"),
    getCarNames.map(s => "^" + s + "$") mkString("|"), 0)

  carsDF.select(
    col("Name"),
  ).where(carNameRegexFilter =!= "")//.show()

  carsDF.select(
    col("Name")
  ).where(carNameFilter).show()

}
