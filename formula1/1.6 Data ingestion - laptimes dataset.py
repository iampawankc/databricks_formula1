# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesting laptimes dataset

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pitstop_schema = StructType(fields=[
    StructField('raceId',IntegerType(), False),
    StructField('driverID',IntegerType(), True),
    StructField('lap',IntegerType(), True),
    StructField('position',IntegerType(), True),
    StructField('time',StringType(), True),
    StructField('milliseconds',IntegerType(), True)
])

# COMMAND ----------

laptimes_df = spark.read\
    .schema(pitstop_schema)\
    .csv("/mnt/formula1kc/raw/lap_times")

# COMMAND ----------

laptimes_df.count()

# COMMAND ----------

laptimes_final_df = laptimes_df.withColumnRenamed('raceId','race_id')\
    .withColumnRenamed('driverID','driver_id')\
    .withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

laptimes_final_df.write\
    .mode("overwrite")\
    .parquet("/mnt/formula1kc/processed/laptimes")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1kc/processed/laptimes"))
