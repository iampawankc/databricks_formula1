# Databricks notebook source
#Using DDL style of defining schema

constructor_schema = "constructorId INT,constructorRef STRING,name STRING,nationality STRING,url STRING"

# COMMAND ----------

constructors_df = spark.read\
    .schema(constructor_schema)\
    .json('/mnt/formula1kc/raw/constructors.json')

constructors_df.printSchema()

# COMMAND ----------

constructors_dropped_df = constructors_df.drop('url')

# COMMAND ----------

from pyspark.sql.functions import current_date

constructors_renamed_df = constructors_dropped_df.withColumnRenamed('constructorId','constructor_id')\
    .withColumnRenamed('constructorRef','constructor_ref')\
    .withColumn('ingestion_date',current_date())

constructors_renamed_df.printSchema()

# COMMAND ----------

constructors_renamed_df.write\
    .mode('overwrite')\
    .parquet('/mnt/formula1kc/processed/constructors')

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1kc/processed/constructors'))
