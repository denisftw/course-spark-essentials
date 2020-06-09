package part2dataframes

import org.apache.spark.sql.SparkSession

object Aggregations extends App {

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")


  import org.apache.spark.sql.functions._
  val genresCountDF = moviesDF.select(count(col("Major_Genre")))
  genresCountDF
//    .show()

  moviesDF.selectExpr("count(distinct(Major_Genre))")
    //.show()

  // including null
  moviesDF.select(count("*"))
//    .show()

  moviesDF.select(countDistinct(col("Major_Genre")))
//    .show()

  moviesDF.select(approx_count_distinct(col("Major_Genre")))
//    .show

  val minRating = moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")
//    .show()

  moviesDF.select(sum(col("US_DVD_Sales")))
//    .show()

  moviesDF.selectExpr("avg(IMDB_Rating) as Average_Rating")
//    .show()

  moviesDF.select(
    mean(col("IMDB_Rating")),
    stddev(col("IMDB_Rating"))
  )
//    .show()

  // Grouping
  val countbyGenreDF = moviesDF.groupBy(col("Major_Genre"))
    .count()
//    .show()

  val avgImdbRatingByGenreDF = moviesDF.groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")
//    .show()


  val aggregationsByGenre = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))

//  aggregationsByGenre.show()

  // 1. Sum all profits of all movies
  moviesDF.select(
    sum(col("US_DVD_Sales")).as("US_DVD_Sales_Sum"),
    sum(col("US_Gross")).as("US_Gross_Sum"),
    sum(col("Worldwide_Gross")).as("Worldwide_Gross_Sum"),
  ).select(
    expr("Worldwide_Gross_Sum + US_Gross_Sum + US_DVD_Sales_Sum").as("All profits")
  ).show()

  // incorrect because of NULLs
  moviesDF.select(
    (col("US_DVD_Sales") + col("US_Gross") + col("Worldwide_Gross")).as("Total")
  ).select(sum(col("Total"))).show()

  // 2. Count number of distinct directors
  moviesDF.select(
    countDistinct(col("Director")).as("Number of directors")
  ).show()

  // 3. mean and std of US gross revenue
  moviesDF.select(
    mean(col("US_Gross")),
    stddev(col("US_Gross"))
  ).show()

  // 4. average IMDB rating and average US gross revenue per director
  val bestDirectorsDF = moviesDF.groupBy(col("Director"))
    .agg(
      avg(col("IMDB_Rating")).as("Avg_IMDB_Rating"),
      avg(col("US_Gross")).as("Avg_US_Gross")
    )

  bestDirectorsDF.drop(col("Avg_IMDB_Rating")).orderBy(expr("0 - Avg_US_Gross")).show()
  bestDirectorsDF.filter(col("Avg_IMDB_Rating").isNotNull).drop(col("Avg_US_Gross")).orderBy(expr("0 - Avg_IMDB_Rating")).show()
  bestDirectorsDF.filter(col("Avg_IMDB_Rating").isNotNull).drop(col("Avg_US_Gross")).orderBy(col("Avg_IMDB_Rating").desc_nulls_last).show()

}
