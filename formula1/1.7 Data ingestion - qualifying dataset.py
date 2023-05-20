# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesting laptimes dataset

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

qualifying_schema = StructType(fields=[
    StructField('qualifyId',IntegerType(), False),
    StructField('raceId',IntegerType(), False),
    StructField('driverId',IntegerType(), True),
    StructField('constructorId',IntegerType(), True),
    StructField('number',IntegerType(), True),
    StructField('position',IntegerType(), True),
    StructField('q1',StringType(), True),
    StructField('q2',StringType(), True),
    StructField('q3',StringType(), True)
])

# COMMAND ----------

qualifying_df = spark.read\
    .schema(qualifying_schema)\
    .option("multiLine", True)\
    .json("/mnt/formula1kc/raw/qualifying")

# COMMAND ----------

qualifying_df.display()

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed('qualifyId','qualify_id')\
    .withColumnRenamed('raceId','race_id')\
    .withColumnRenamed('driverID','driver_id')\
    .withColumnRenamed('constructorId','constructorId')\
    .withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

qualifying_final_df.write\
    .mode("overwrite")\
    .parquet("/mnt/formula1kc/processed/qualifying")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1kc/processed/qualifying"))
