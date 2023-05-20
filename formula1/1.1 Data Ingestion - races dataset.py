# Databricks notebook source
# MAGIC %md
# MAGIC ## Races dataset

# COMMAND ----------

races_df = spark.read.csv("/mnt/formula1kc/raw/races.csv", header=True)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False), 
    StructField("year", IntegerType(), False), 
    StructField("round", IntegerType(), False), 
    StructField("circuitId", IntegerType(), False), 
    StructField("name", StringType(), False), 
    StructField("date", StringType(), False), 
    StructField("time", StringType(), False),
    StructField("url", StringType(), False) 
])

# COMMAND ----------

races_df = spark.read \
    .schema(races_schema) \
    .csv("/mnt/formula1kc/raw/races.csv", header=True)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

races_selected_df = races_df.select(col("raceId").alias("race_id"),col("year"),col("round"),col("circuitId").alias("circuit_id"),col("name"),col("date"),col("time"))
races_selected_df.printSchema()

# COMMAND ----------

# races_ranamed_df = races_selected_df.withColumnRenamed("raceId","race_id") \
#     .withColumnRenamed("circuitId","circuit_id")

# races_ranamed_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, lit, col, current_timestamp

races_ranamed_df = races_ranamed_df \
    .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(' '), col("time")), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

races_ranamed_df.printSchema()

# COMMAND ----------

display(races_ranamed_df)

# COMMAND ----------

races_final_df = races_ranamed_df.select(col('race_id'), col('year').alias('race_year'), col('round'), col('circuit_id'), col('name'),col('race_timestamp'),col('ingestion_date'))
races_final_df.printSchema()

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

races_final_df.write.mode("overwrite")\
    .partitionBy('race_year')\
    .parquet("/mnt/formula1kc/processed/races")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1kc/processed/races"))
