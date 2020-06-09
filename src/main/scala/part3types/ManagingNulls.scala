package part3types

import org.apache.spark.sql.SparkSession

object ManagingNulls extends App {

  val spark = SparkSession.builder()
    .appName("Managing Nulls")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  import org.apache.spark.sql.functions._
  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
      .as("Fixed_Rating")
  )
//    .show()

  // checking for nulls
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNotNull)
  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)

  // removing nulls
  moviesDF.select("Title", "IMDB_Rating").na.drop()

  // replacing nulls
  moviesDF.na.fill(0, Seq("IMDB_Rating", "Rotten_Tomatoes_Rating"))

  moviesDF.na.fill(Map(
    "IMDB_Rating" -> 0, "Rotten_Tomatoes_Rating" -> 10,
    "Director" -> "Unknown"
  ))

  // complex operations
  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull",
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl",
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif",
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2",
  ).show()

}
