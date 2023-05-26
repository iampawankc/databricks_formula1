# Databricks notebook source
# MAGIC %md
# MAGIC # Applying transformations on all dataframes

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1 : Reading all data to dataframes

# COMMAND ----------

results_df = spark.read.parquet("/mnt/formula1kc/processed/results")\
    .withColumnRenamed("time","race_time")

# COMMAND ----------

drivers_df = spark.read.parquet("/mnt/formula1kc/processed/drivers")\
    .withColumnRenamed("name","driver_name")\
    .withColumnRenamed("number","driver_number")\
    .withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

constructors_df = spark.read.parquet("/mnt/formula1kc/processed/constructors")\
    .withColumnRenamed("name","team")

# COMMAND ----------

circuits_df = spark.read.parquet("/mnt/formula1kc/processed/circuits")\
    .withColumnRenamed("location", "circuit_location")

# COMMAND ----------

races_df = spark.read.parquet("/mnt/formula1kc/processed/races")\
    .withColumnRenamed("name","race_name")\
    .withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2: Applying joins

# COMMAND ----------

race_circuit_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")\
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

race_results_df = results_df.join(race_circuit_df, results_df.race_id == race_circuit_df.race_id, "inner")\
    .join(constructors_df, constructors_df.constructor_id == results_df.constructor_id, "inner")\
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner")\

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

final_df = race_results_df.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number","driver_nationality", "team", "grid", "fastest_lap", "race_time", "points")\
    .withColumn("created_date", current_timestamp())

# COMMAND ----------

display(final_df.filter("race_year=2020 and race_name='Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3: Writing the data back to presentation container

# COMMAND ----------

final_df.write.mode("overwrite")\
    .parquet("/mnt/formula1kc/presentation/race_results")
