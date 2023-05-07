# Databricks notebook source
# MAGIC %md
# MAGIC #1. Plain data ingestion

# COMMAND ----------

circuits_df = spark.read.csv("/mnt/formula1kc/raw/circuits.csv", header=True)

#circuits_df = spark.read.csv("/mnt/formula1kc/raw/circuits.csv", header=True, inferSchema=True)
#inferSchema is used to read the schema/datatype information of each column in csv/source and use the same type in df rather than string
#inferSchema is not efficient for large/production datasets


# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #2. Working with schema

# COMMAND ----------

# Getting the schema of the dataframe
circuits_df.printSchema()

# COMMAND ----------

#Getting statistical info(min, max, stddev,.. ) of the dataframe
display(circuits_df.describe())

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuits_schema = StructType( fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), False),
    StructField("name", StringType(), False),
    StructField("country", StringType(), False),
    StructField("lat", DoubleType(), False),
    StructField("lng", DoubleType(), False),
    StructField("alt", DoubleType(), False),
    StructField("url", StringType(), False),
])

# StructType is the container
# StructField defines each column of the dataframe, syntax(Column_name, datatype, can be null(bool value))

# COMMAND ----------

circuits_df = spark.read\
    .option("header", True)\
    .schema(circuits_schema)\
    .csv("/mnt/formula1kc/raw/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #3. Selecting only required columns from dataframe

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId","circuitRef","name","country","lat","lng","alt")

#Other ways of selecting columns
# circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, ....)
# circuits_selected_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], ... )

# from pyspark.sql.functions import col #Recommended approach
# circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"))

# COMMAND ----------

circuits_selected_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #4. Ranaming columns

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude") 

# COMMAND ----------

circuits_renamed_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #5. Adding new columns to dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())

#Any column to be added to df should a Column object, to add literal use lit() defined in lit import

# COMMAND ----------

circuits_final_df.printSchema()

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #6. Writing processed data back to datalake

# COMMAND ----------

# Writing processed data back to datalake as parquet file

circuits_final_df.write \
    .mode("overwrite") \
    .parquet("/mnt/formula1kc/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /mnt/formula1kc/processed/circuits

# COMMAND ----------

test_df = spark.read.parquet("/mnt/formula1kc/processed/circuits")
display(test_df)
