// Databricks notebook source
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
val departmentsSchema = StructType(
  Array(
    StructField("ID", DecimalType(10,0), true),
    StructField("DEPARTMENT", StringType, true)
  )
)
val jobsSchema = StructType(
  Array(
    StructField("ID", DecimalType(10,0), true),
    StructField("JOB", StringType, true)
  )
)
val hiredEmployeesSchema = StructType(
  Array(
    StructField("ID", DecimalType(10,0), true),
    StructField("NAME", StringType, true),
    StructField("DATETIME", StringType, true),
    StructField("DEPARTMENT_ID_ID", DecimalType(10,0), true),
    StructField("JOB_ID_ID", DecimalType(10,0), true),
  )
)
val departmentsRejectedSchema = StructType(
  Array(
    StructField("ID", DecimalType(10,0), true),
    StructField("DEPARTMENT", StringType, true)
  )
)
val jobsRejectedSchema = StructType(
  Array(
    StructField("ID", DecimalType(10,0), true),
    StructField("JOB", StringType, true)
  )
)
val hiredRejectedEmployeesSchema = StructType(
  Array(
    StructField("ID", DecimalType(10,0), true),
    StructField("NAME", StringType, true),
    StructField("DATETIME", StringType, true),
    StructField("DEPARTMENT_ID_ID", DecimalType(10,0), true),
    StructField("JOB_ID_ID", DecimalType(10,0), true),
  )
)

// COMMAND ----------

val tables = List(
    Map("file"-> "departments.csv", "table"-> "EMPLOYEES_DEPARTMENTS", "nulls_table" -> "EMPLOYEES_DEPARTMENTSREJECTED", "schema"-> departmentsSchema),
    Map("file"-> "jobs.csv", "table"-> "EMPLOYEES_JOBS", "nulls_table" -> "EMPLOYEES_JOBSREJECTED", "schema"-> jobsSchema),
    Map("file"-> "hired_employees.csv", "table"-> "EMPLOYEES_HIREDEMPLOYEES", "nulls_table" -> "EMPLOYEES_HIREDEMPLOYEESREJECTED", "schema"-> hiredEmployeesSchema)
)
val SfOptions = Map(
    "sfURL" -> "",
    "sfUser" -> "",
    "sfPassword" -> "",
    "sfDatabase" -> "globant",
    "sfSchema" -> "employees",
    "sfWarehouse" -> "COMPUTE_WH"
)

val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

// COMMAND ----------

def readFile(file: String, customSchema: StructType): DataFrame = {
  // File location and type
  val fileLocation = s"/FileStore/tables/globant/${file}"
  val fileType = "csv"

  // CSV options
  val inferSchema = "true"
  val firstRowIsHeader = "false"
  val delimiter = ","

  // The applied options are for CSV files. For other file types, these will be ignored.
  spark.read.format(fileType)
    .option("header", firstRowIsHeader)
    .option("sep", delimiter)
    .schema(customSchema)
    .load(fileLocation)
}

// COMMAND ----------

def span(df: DataFrame): (DataFrame, DataFrame) = {
  val dfWithNulls = df.filter(_.anyNull)
  val dfWithoutNulls = df.filter(!_.anyNull)
  (dfWithoutNulls, dfWithNulls)
}

// COMMAND ----------

def writeIntoSnowflake(df: DataFrame, table: String): Unit =
  df.write
    .format(SNOWFLAKE_SOURCE_NAME)
    .options(SfOptions)
    .option("dbtable", table)
    .mode("overwrite")
    .save()

// COMMAND ----------

def uploadData(map: Map[String, java.io.Serializable]): Unit = {
  val df = readFile(map("file").toString, map("schema").asInstanceOf[StructType])
  val (dfOkay, dfRejected) = span(df)
  assert(df.count() == dfOkay.count() + dfRejected.count())
  writeIntoSnowflake(dfRejected, map("nulls_table").toString)
  writeIntoSnowflake(dfOkay, map("table").toString)
}

// COMMAND ----------

tables.foreach(uploadData)

// COMMAND ----------

