package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DecimalType, DoubleType, LongType, StringType, StructField, StructType}
import part2dataframes.DataFramesBasics.spark

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Format")
    .config("spark.master", "local")
    .getOrCreate()

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

  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode", "failFast")
    .option("path", "src/main/resources/data/cars.json")
    .load()

  carsDF.show()


    val carsDFUsingOptionMap = spark.read
      .format("json")
      .options(Map(
        "mode" -> "failFast",
        "path" -> "src/main/resources/data/cars.json",
        "inferSchema" -> "true"
      ))
      .load()

    carsDF.write
      .format("json")
      .mode(SaveMode.Overwrite)
      .options(Map(
        "path" -> "src/main/resources/data/cars-written.json"
      ))
      .save()


    val withOptions = spark.read
      .format("json")
      .schema(carsSchema)
      .option("dateFormat", "yyyy-MM-dd")
      .option("allowSingleQuotes", "true")
      .option("compression", "uncompressed")
      .option("path", "src/main/resources/data/cars.json")
      .load()
    withOptions.show()




  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("data", DateType),
    StructField("price", DoubleType)
  ))

  val stocks = spark.read
    .schema(stocksSchema)
    .format("csv")
    .option("path", "src/main/resources/data/stocks.csv")
    .option("dateFormat", "MMM d yyyy")
    .option("sep", ",")
    .option("nullValue", "")
    .load()

  stocks.show()

  carsDF.write
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .options(Map(
      "path" -> "src/main/resources/data/cars-written.parquet"
    ))
    .save()

  spark.read
    .text("src/main/resources/data/sampleTextFile.txt")
    .show()

  val employees = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employees.show()

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  moviesDF.show()

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .option("nullValue", "NULL")
    .option("path", "src/main/resources/data/movies-out.csv")
    .save()


  moviesDF.write
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .options(Map(
      "path" -> "src/main/resources/data/movies-out.parquet"
    ))
    .save()

  moviesDF.write
    .format("jdbc")
    .mode(SaveMode.Overwrite)
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .save()


}
