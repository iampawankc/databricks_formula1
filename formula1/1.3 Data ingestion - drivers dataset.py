# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

driver_name_schema = StructType(fields=[
    StructField('forename',StringType(),False),
    StructField('surname',StringType(),False),
])

drivers_schema = StructType(fields=[
    StructField('driverId', IntegerType(), False),
    StructField('driverRef', StringType(), False),
    StructField('number', IntegerType(), False),
    StructField('code', StringType(), False),
    StructField('name', driver_name_schema),
    StructField('dob', DateType(), False),
    StructField('nationality', StringType(), False),
    StructField('url', StringType(), False),
])

# COMMAND ----------

drivers_df = spark.read\
    .schema(drivers_schema)\
    .json('/mnt/formula1kc/raw/drivers.json')

drivers_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit

drivers_renamed_df = drivers_df.withColumnRenamed('driverId','driver_id')\
    .withColumnRenamed('driverRef', 'driver_ref')\
    .withColumn('ingestion_date', current_timestamp())\
    .withColumn('name', concat('name.forename', lit(' '), 'name.surname'))

drivers_renamed_df.printSchema()

# COMMAND ----------

drivers_final_df = drivers_renamed_df.drop('url')

# COMMAND ----------

drivers_final_df.printSchema()

# COMMAND ----------

drivers_final_df.write.mode('overwrite').parquet('/mnt/formula1kc/processed/drivers')

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1kc/processed/drivers'))
