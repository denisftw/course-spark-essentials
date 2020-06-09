package part2dataframes

import org.apache.spark.sql.SparkSession

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")

  guitaristsDF.join(bandsDF, joinCondition, "left")
//    .show()
  guitaristsDF.join(bandsDF, joinCondition, "right")
//    .show()

  guitaristsDF.join(bandsDF, joinCondition, "full")
//    .show()

  // semi-joins
  guitaristsDF.join(bandsDF, joinCondition, "left_semi")
//    .show()

  // anti-joins
  guitaristsDF.join(bandsDF, joinCondition, "left_anti")
//    .show()

  guitaristsDF.join(bandsDF.
    withColumnRenamed("id", "band"), "band")
//    .show()

  import org.apache.spark.sql.functions._
  guitaristsDF.join(guitarsDF.withColumnRenamed(
    "id", "guitarId"),
    expr("array_contains(guitars, guitarId)"))
//    .show()



  // 1. show all employees and their max salaries
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
//
//  employeesDF.show()
//  deptManagersDF.show()
//  titlesDF.show()
//  salariesDF.show()


  /*
  --   // 1. show all employees and their max salaries
select * from employees e join (
select emp_no, max(salary) as max_salary from salaries
    group by emp_no) s on e.emp_no = s.emp_no;
   */

  val maxSalaryDF = salariesDF.groupBy(col("emp_no")).agg(
    max(col("salary")).as("max_salary"))

  val employeeMaxSalaryDF = employeesDF.join(maxSalaryDF,
    employeesDF.col("emp_no") === maxSalaryDF.col("emp_no"))
//    .drop(maxSalaryDF.col("emp_no"))

//  employeeMaxSalaryDF.show()


  // 2. show all employees who were never managers

  /*
  --   // 2. show all employees who were never managers
select * from employees where emp_no not in (select emp_no from dept_manager);
   */

  val nonManagers = employeesDF.join(deptManagersDF, employeesDF.col("emp_no") ===
    deptManagersDF.col("emp_no"), "left_anti")

  println(nonManagers.count())



  // 3. find job titles (latest) of the 10 best paid employees (table titles)

  /*
  select f.emp_no, ss.title from (select e.emp_no, s.max_salary from employees e join (
    select emp_no, max(salary) as max_salary from salaries
    group by emp_no) s on e.emp_no = s.emp_no
order by s.max_salary desc limit 10) f join (
    select t.emp_no, title from titles t join (
        select emp_no, max(from_date) as last_start_date from titles group by emp_no)
        lt on t.emp_no = lt.emp_no and t.from_date = lt.last_start_date
    ) ss on f.emp_no = ss.emp_no;
   */

  val lastTitleStartsDF = titlesDF.groupBy(col("emp_no")).agg(max(col("from_date")).as("last_from_date"))

//  lastTitleStartsDF.show()

  val lastTitlesDF = titlesDF.join(lastTitleStartsDF,
    titlesDF.col("emp_no") === lastTitleStartsDF.col("emp_no") and
    titlesDF.col("from_date") === lastTitleStartsDF.col("last_from_date"))
      .drop(titlesDF.col("emp_no"))

  lastTitlesDF.show()

  val topSalaryDF = maxSalaryDF.orderBy(col("max_salary").desc).limit(10)

  topSalaryDF.show()

  val topTitlesDF = topSalaryDF.join(lastTitlesDF, topSalaryDF.col("emp_no") === lastTitlesDF.col("emp_no"))

  topTitlesDF.show()




}
