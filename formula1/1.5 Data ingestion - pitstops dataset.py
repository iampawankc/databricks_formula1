# Databricks notebook source
# MAGIC %md
# MAGIC # Pitstop dataset
# MAGIC
# MAGIC 1. Import the pitstop dataset and make necessary modifications, cleaning
# MAGIC 2. Add new column ingestion_date to the dataset 
# MAGIC 3. Save the dataset back to data lake as parquet file with partitioning enabled
# MAGIC 3. This is a multiline JSON file unlike other files we loaded earlier

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

pitstop_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("stop", StringType(), False),
    StructField("lap", IntegerType(), False),
    StructField("time", StringType(), False),
    StructField("duration", StringType(), False),
    StructField("milliseconds", IntegerType(), False)
])

# COMMAND ----------

lap_df = spark.read\
    .schema(pitstop_schema)\
    .option("multiline", True)\
    .json('/mnt/formula1kc/raw/pit_stops.json')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

lap_renamed_df = lap_df.withColumnRenamed('raceId','race_id')\
    .withColumnRenamed('driverId','driver_id')\
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

lap_renamed_df.write\
    .mode("overwrite")\
    .parquet("/mnt/formula1kc/processed/pit_stop")

# COMMAND ----------

spark.read.parquet("/mnt/formula1kc/processed/pit_stop").display()
