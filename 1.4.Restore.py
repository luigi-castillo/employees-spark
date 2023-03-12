# Databricks notebook source
tables = [
    "EMPLOYEES_DEPARTMENTS",
    "EMPLOYEES_JOBS",
    "EMPLOYEES_HIREDEMPLOYEES",
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

for table in tables:
    df = spark.read.format("avro") \
      .load("/FileStore/globant002/{}.avro".format(table))
    df.write\
      .format(SNOWFLAKE_SOURCE_NAME)\
      .options(**SfOptions)\
      .option("dbtable", table)\
      .mode("overwrite")\
      .save()

# COMMAND ----------

