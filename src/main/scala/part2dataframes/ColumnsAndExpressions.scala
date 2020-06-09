package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.expressions.Expression
import org.apache.spark.sql.types.DoubleType

object ColumnsAndExpressions extends App{

  val spark = SparkSession.builder()
    .appName("DF columns and expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .option("path", "src/main/resources/data/cars.json")
    .load()

//  carsDF.show()

  // Columns
  val firstColumn = carsDF.col("Name")

  // selecting (projecting)
  val carNamesDF = carsDF.select(firstColumn)
//  carNamesDF.show()

  // other select methods
  import org.apache.spark.sql.functions._
  import spark.implicits._
  carsDF.select(
    col("Name"),
    column("Acceleration"),
    'Year,
    $"Horsepower",
    expr("Origin")
  )
//    .show()

//  carsDF.select("Name", "Year").show()

  // Selection is an example of `narrow` transformation

  // EXPRESSIONS

  val weightInLbsExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    weightInLbsExpression,
    weightInKgExpression.as("Weight in KG"),
    expr("Weight_in_lbs / 2.2")
  )
//  carsWithWeightsDF.show()


  // shortcut
  val carsWithSelectExprWeightDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  ).show()

  // Processing
  val carsWithKgDF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)

  // Renaming
  val carsWithSpacedColumnsDF = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")

  carsWithSpacedColumnsDF.selectExpr("`Weight in pounds`")

  // Removing
  carsDF.drop("Name")

  // Filtering
  val nonAmericanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val americanCarsWhereDF = carsDF.where(col("Origin") === "USA")
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  val americanPowerCars1DF = carsDF
    .filter(col("Origin") === "USA")
    .filter(col("Horsepower") > 220)

  val americanPowerCarsDF = carsDF.filter(
    "Origin = 'USA' and Horsepower > 220"
  )

  val americanPowerCars2DF = carsDF.filter(
    (col("Origin") === "USA").and(col("Horsepower") > 220))

  americanPowerCarsDF.show()

  val moreCarsDF = spark.read.option("inferSchema", "true").
    json("src/main/resources/data/more_cars.json")

  val allCarsFD = carsDF.union(moreCarsDF)

  carsDF.select("Origin").distinct()
//    .show()


  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  // 1. select 2 any columns from the movies data frame
  moviesDF.select(
    col("Title"),
    column("US_Gross"),
    column("Worldwide_Gross"),
    column("US_DVD_Sales"),
    $"Major_Genre",
    expr("IMDB_Rating")
  ).show()


  // 2. create new DF by summing all profits as Total profit
  moviesDF.printSchema()

  val usDvdSalesColumn = col("US_DVD_Sales")
  moviesDF.withColumn("Total_profit", col("US_Gross") + col("Worldwide_Gross")+
    when(usDvdSalesColumn.isNull, 0).otherwise(usDvdSalesColumn))
//    .show()

  moviesDF.filter(usDvdSalesColumn.isNotNull).withColumn("Total_profit", col("US_Gross") + col("Worldwide_Gross")+
    when(usDvdSalesColumn.isNull, 0).otherwise(usDvdSalesColumn))
//    .show()

  moviesDF
      .withColumn("Total_profit", expr("coalesce(US_DVD_Sales, 0) + coalesce(US_Gross, 0) + coalesce(Worldwide_Gross, 0)"))
      .show()

  moviesDF
    .withColumn("US_DVD_Sales_NN", expr("coalesce(US_DVD_Sales, 0)"))
    .withColumn("US_Gross_NN", expr("coalesce(US_Gross, 0)"))
    .withColumn("Worldwide_Gross_NN", expr("coalesce(Worldwide_Gross, 0)"))
    .withColumn("Total_profit", col("US_DVD_Sales_NN") + col("US_Gross_NN") + col("Worldwide_Gross_NN"))
    .drop("US_DVD_Sales_NN", "US_Gross_NN", "Worldwide_Gross_NN")
//    .show()

  // 3. select all comedy movies `Major_genre` with IMDB rating > 6

  moviesDF.filter((col("Major_Genre") === "Comedy").and(col("IMDB_Rating") > 6))
//    .show()
  moviesDF.filter("Major_Genre = 'Comedy' and IMDB_Rating > 6").orderBy(col("IMDB_Rating").desc).show()

}
