package part3types

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructField, StructType}

object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()


  val moviesDF = spark
    .read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  import org.apache.spark.sql.functions._


  val firstDateConv = to_date(col("Release_Date"), "d-MMM-yy")
  val secondDateConv = to_date(col("Release_Date"), "yyyy-MM-dd")
  val multiFormat = when(firstDateConv.isNotNull, firstDateConv).otherwise(secondDateConv)

  val multiFormat2 = firstDateConv

  val moviesWithReleaseDatesDF = moviesDF.select(
    col("Title"),
    multiFormat.as("Actual_Release")
  )

  moviesWithReleaseDatesDF.filter(col("Title") === "55 Days at Peking")
//    .show()

  moviesWithReleaseDatesDF.withColumn("Today", current_date())
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_Age",
      datediff(col("Today"), col("Actual_Release")) / 365)
//    .show()


  /*
   1. How to deal with changing formats
   2. Read stocks DF and parse the dates
   */

  val stocksDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")

  stocksDF.select("*").withColumn("Real_Date",
    to_date(col("date"), "MMM d yyyy"))
//    .show()


  moviesDF.select(col("Title"),
    struct(col("US_Gross"), col("Worldwide_Gross"))
      .as("Profit")).
    select(col("Profit").getField("US_Gross"))
//    .show()

  moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross").show()

  // Arrays
  moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_Words"))
    .select(col("Title"), expr("Title_Words[0]"),
      size(col("Title_Words")),
      array_contains(col("Title_Words"), "Love"))
    .show()

}
