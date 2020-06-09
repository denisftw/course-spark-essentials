package part5rdds

import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

object Rdds extends App {

  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext

  // 1 parallelize existing collections

  val numbers = 1 to 1000000
  val numbersRDD = sc.parallelize(numbers)

  // 2 - reading from files
  case class StockValue(symbol: String, date: String, price: Double)
  def readStocks(filename: String) = {
    Source.fromFile(filename).getLines().drop(1).map { line =>
      line.split(",")
    }.map { tokens =>
      StockValue(tokens(0), tokens(1), tokens(2).toDouble)
    }.toList
  }

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2b reading from files
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map { str => str.split(",") }
    .filter(tokens => tokens(0).toUpperCase == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  println(stocksRDD2)

  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._
  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd

  // RDD -> DF
  val numbersDF = numbersRDD.toDF("numbers")  // lose type info

  // RDD -> DS
  val numbersDS = spark.createDataset(numbersRDD) // keep type info


  // Transformations
  val msftRdd = stocksRDD.filter(_.symbol == "MSFT")  // lazy (transformation)
  val msftCount = msftRdd.count()                     // eager (action)

  val companyNames = stocksRDD.map(_.symbol).distinct()   // both lazy

  // min max
  implicit val stockOrdering = Ordering.fromLessThan[StockValue](_.price < _.price)
  val minMsft = msftRdd.min()       // eager action

  // reduce
  numbersRDD.reduce(_ + _)

  // grouping
  val symbolStocksRdd = stocksRDD.groupBy(_.symbol)   // expensive

  // Partitioning
  val repartitionedStocksRDD = stocksRDD.repartition(30)
  repartitionedStocksRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stock30")
  // Size of a partition should be 10 - 100Mb

  // coalescing
  val coalescedRDD = repartitionedStocksRDD.coalesce(15)
  coalescedRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stock15")

  case class Movie(title: String, genre: String, rating: Double)
  /*
  1. Read the movies.json as an RDD
  2. Show the distinct genres as an RDD
  3. Select all the movie from the drama genre with IMDB_Rating > 6
  4. Show the average rating of movies by genre (using RDD)
   */

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  import org.apache.spark.sql.functions._
  val columnizedMoviesDF = moviesDF.select(
    col("Title").as("title"),
    coalesce(col("Major_Genre"), lit("n/a")).as("genre"),
    coalesce(col("IMDB_Rating"), lit(-1)).as("rating")
  )

//  columnizedMoviesDF.show()

  val moviesDS = columnizedMoviesDF.as[Movie]
  val moviesRDD = moviesDS.rdd

  val genres = moviesRDD.map(_.genre).distinct().filter(_ != "n/a").collect()

  val goodDramas = moviesRDD.filter(m => m.genre == "Drama" && m.rating > 6).collect()

  val aggregatedMovies = moviesRDD.filter(_.genre != "n/a").groupBy(_.genre)

  val ratings = moviesRDD.filter(_.genre != "n/a").groupBy(_.genre).aggregateByKey(0.0)(
    (acc, movies) => {
      val withoutNegatives = movies.filter(_.rating > 0).map(_.rating)
      val collectionSize = withoutNegatives.size
      if (collectionSize != 0) acc + withoutNegatives.sum / collectionSize else acc
    }, (a, b) => a + b).collect()

  val reference = moviesRDD.filter(_.genre != "n/a").groupBy(_.genre).map { case (genre, movies) =>
    val withoutNegatives = movies.filter(_.rating > 0).map(_.rating)
    val collectionSize = withoutNegatives.size
    val avg = if (collectionSize != 0)  withoutNegatives.sum / collectionSize else 0
    (genre -> avg)
  }.collect()

  moviesDS.show()


}
