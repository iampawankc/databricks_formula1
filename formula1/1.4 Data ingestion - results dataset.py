# Databricks notebook source
# Import the dataset and make necessary modifications, cleaning
# Add new column ingestion_date to the dataset 
# Save the dataset back to data lake as parquet file with partitioning enabled (race_id)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, FloatType

results_schema = StructType(fields=[
    StructField('constructorId', IntegerType(), False),
    StructField('driverId', IntegerType(), False),
    StructField('fastestLap', StringType(), False),
    StructField('fastestLapTime', StringType(), False),
    StructField('fastestLapSpeed', FloatType(), False),
    StructField('grid', IntegerType(), False),
    StructField('laps', IntegerType(), False),
    StructField('milliseconds', IntegerType(), False),
    StructField('number', IntegerType(), False),
    StructField('points', FloatType(), False),
    StructField('position', IntegerType(), False),
    StructField('positionOrder', IntegerType(), False),
    StructField('positionText', StringType(), False),
    StructField('raceId', IntegerType(), False),
    StructField('rank', IntegerType(), False),
    StructField('resultId', IntegerType(), False),
    StructField('statusId', StringType(), False),
    StructField('time', StringType(), False), 
])

# COMMAND ----------

results_df = spark.read\
    .schema(results_schema)\
    .json('/mnt/formula1kc/raw/results.json')

results_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

results_renamed_df = results_df.withColumnRenamed('constructorId','constructor_id')\
    .withColumnRenamed('driverId','driver_id')\
    .withColumnRenamed('fastestLap','fastest_lap')\
    .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed')\
    .withColumnRenamed('fastestLapTime', 'fastest_lap_time')\
    .withColumnRenamed('positionOrder','position_order')\
    .withColumnRenamed('positionText','position_text')\
    .withColumnRenamed('raceId','race_id')\
    .withColumnRenamed('resultId','result_id')\
    .withColumnRenamed('statusId','status_id')\
    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

from pyspark.sql.functions import col

results_final_df = results_renamed_df.drop(col('status_id'))

# COMMAND ----------

results_final_df.write\
    .mode('overwrite')\
    .partitionBy('race_id')\
    .parquet('/mnt/formula1kc/processed/results')

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1kc/processed/results'))
