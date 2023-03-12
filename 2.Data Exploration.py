# Databricks notebook source
tables = [
    "EMPLOYEES_HIREDEMPLOYEES",
    "EMPLOYEES_DEPARTMENTS",
    "EMPLOYEES_JOBS",
]
SfOptions = {
  "sfURL" : "",
  "sfUser" : "",
  "sfPassword" : "",
  "sfDatabase" : "globant",
  "sfSchema" : "employees",
  "sfWarehouse" : "COMPUTE_WH"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

# COMMAND ----------

from pyspark.sql.functions import *
hired_employees_raw = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
    .options(**SfOptions) \
    .option("query",  "select * from EMPLOYEES_HIREDEMPLOYEES") \
    .load()
departments = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
    .options(**SfOptions) \
    .option("query",  "select * from EMPLOYEES_DEPARTMENTS") \
    .load()
jobs = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
    .options(**SfOptions) \
    .option("query",  "select * from EMPLOYEES_JOBS") \
    .load()

# COMMAND ----------

hired_employees_w_employee_id_field = hired_employees_raw.withColumnRenamed("ID", "EMPLOYEE_ID")
hired_employees = hired_employees_w_employee_id_field\
                    .join(departments, hired_employees_w_employee_id_field.DEPARTMENT_ID_ID == departments.ID, "inner")\
                    .join(jobs, hired_employees_w_employee_id_field.JOB_ID_ID == jobs.ID, "inner")\
                    .select("EMPLOYEE_ID", "NAME", "DATETIME", "DEPARTMENT", "JOB", "DEPARTMENT_ID_ID")\
                    .withColumn("YEAR", year(col("DATETIME")))

# COMMAND ----------

# DBTITLE 1,Requirement 1
hired_employees_w_quarter = hired_employees\
                            .withColumn("QUARTER", concat(lit("Q"),quarter(col("DATETIME"))))\
                            .orderBy(col("DEPARTMENT"), col("JOB"))
display(hired_employees_w_quarter)

# COMMAND ----------

# DBTITLE 0,Requirement 1
requirement1 = hired_employees_w_quarter\
                .filter(col("YEAR") == "2021")\
                .groupBy(col("DEPARTMENT"), col("JOB")).pivot("QUARTER").count().na.fill(0)\
                .orderBy(col("DEPARTMENT"), col("JOB"))
display(requirement1)

# COMMAND ----------

# DBTITLE 1,Requirement 2
hired_per_department = hired_employees\
        .filter(col("YEAR") == "2021")\
        .groupBy(
            col("DEPARTMENT_ID_ID").alias("DEPARTMENT_ID")
        ).agg(
            first(col("DEPARTMENT")).alias("DEPARTMENT"),
            count("EMPLOYEE_ID").alias("HIRED")
        )
display(hired_per_department)

# COMMAND ----------

mean = hired_per_department.agg(avg("HIRED")).first()[0]
display(mean)

# COMMAND ----------

# DBTITLE 0,Requirement 2
requirement2 = hired_per_department\
    .filter(col("HIRED") > mean)\
    .orderBy(desc(col("HIRED")))
display(requirement2)

# COMMAND ----------

