# Databricks notebook source
race_results_df = spark.read.parquet("/mnt/formula1kc/presentation/race_results/")

# COMMAND ----------

demo_df = race_results_df.filter(race_results_df.race_year==2020)

# COMMAND ----------

demo_df.count()

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum 

# COMMAND ----------

demo_df.select(count('*')).show()

# COMMAND ----------

demo_df.select(count(demo_df.race_name!='NULL')).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'")\
    .select(sum("points"), \
    countDistinct("race_name"))\
    .withColumnRenamed("sum(points)", "total_points")\
    .withColumnRenamed("count(DISTINCT race_name)", "number_of_races")\
    .show()
