package part4sql

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import part2dataframes.Joins.spark

object SparkSql extends App {
  val spark = SparkSession.builder()
    .appName("Spark SQL Practice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  import org.apache.spark.sql.functions._
  carsDF.select(col("Name")).where(col("Origin") === "USA")

  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin)

  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  val databasesDF = spark.sql("show databases")
  databasesDF.show()

  // transfer tables from a DB to Spark tables
  val optionMap = Map(
    ("driver", "org.postgresql.Driver"),
    ("url", "jdbc:postgresql://localhost:5432/rtjvm"),
    ("user", "docker"),
    ("password", "docker")
  )
  val employeesDF = spark.read
    .format("jdbc")
    .options(optionMap)
    .option("dbtable", "public.employees")
    .load()
  val salariesDF = spark.read
    .format("jdbc")
    .options(optionMap)
    .option("dbtable", "public.salaries")
    .load()
  val titlesDF = spark.read
    .format("jdbc")
    .options(optionMap)
    .option("dbtable", "public.titles")
    .load()
  val deptManagersDF = spark.read
    .format("jdbc")
    .options(optionMap)
    .option("dbtable", "public.dept_manager")
    .load()
  val deptEmpDF = spark.read
    .format("jdbc")
    .options(optionMap)
    .option("dbtable", "public.dept_emp")
    .load()
  val departmentsDF = spark.read
    .format("jdbc")
    .options(optionMap)
    .option("dbtable", "public.departments")
    .load()

  def transferTables(dataFrames: Seq[(String, DataFrame)]): Unit = {
    dataFrames.foreach { case (name, dataFrame) =>
      dataFrame.createOrReplaceTempView(name)
      dataFrame.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(name)
    }
  }

  def createTableViews(dataFrames: Seq[(String, DataFrame)]): Unit = {
    dataFrames.foreach { case (name, dataFrame) =>
      dataFrame.createOrReplaceTempView(name)
    }
  }

  createTableViews(Seq(
    "employees" -> employeesDF,
    "salaries" -> salariesDF,
    "titles" -> titlesDF,
    "dept_manager" -> deptManagersDF,
    "dept_emp" -> deptEmpDF,
    "departments" -> departmentsDF,
  ))

  val employeesDF2 = spark.read.table("employees")

  /**
   * 1. Read the movies DF and store as a Spark table in rtjvm DB
   * 2. Count how many employees we had between 01/01/1999 and 01/01/2001 (hire_date)
   * 3. Show the average salaries for the employees hired between those dates grouped by dept_number
   * 4. Show the name of the best paying department between those dates
   */

    val moviesDF = spark.read
      .option("inferSchema", "true")
      .json("src/main/resources/data/movies.json")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("movies")

  val employeesWithinTimeDF = spark.sql(
    "select * from employees where hire_date >= '1999-01-01' and hire_date <= '2001-01-01'")
  println(s"Employees within time frame: ${employeesWithinTimeDF.count()}")
  employeesWithinTimeDF.createOrReplaceTempView("employees_within")

  val employeesSalaryDepartments = spark.sql(
    """
      |select ew.*, d.*, s.salary from employees_within ew inner join dept_emp de on ew.emp_no = de.emp_no
      | inner join departments d on de.dept_no = d.dept_no
      | inner join salaries s on ew.emp_no = s.emp_no
      |""".stripMargin)
  employeesSalaryDepartments.createOrReplaceTempView("department_employees")

  val avgSalariesByDepartmentDF = spark.sql(
    """
      |select avg(salary) as avg_salary, dept_no, dept_name from department_employees group by dept_no, dept_name
      |order by avg_salary desc
      |""".stripMargin)
  avgSalariesByDepartmentDF.createOrReplaceTempView("avg_salaries_departments")
//  avgSalariesByDepartmentDF.show()


  val mostPayingDept = spark.sql(
    """
      |select dept_name from avg_salaries_departments where
      |avg_salary = (select max(avg_salary) from avg_salaries_departments)
      |""".stripMargin)

  mostPayingDept.show()


}
