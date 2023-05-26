# Databricks notebook source
# MAGIC %md
# MAGIC # Using filters

# COMMAND ----------

races_df = spark.read.parquet("/mnt/formula1kc/processed/races")\
    .withColumnRenamed("name","race_name")

# COMMAND ----------

# SQL way
races_df.filter("race_year = 2019 and round < 5").display()

# COMMAND ----------

# Python way
races_df.filter((races_df["race_year"] == 2019) & (races_df["round"] <=5 )).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Inner join

# COMMAND ----------

circuits_df = spark.read.parquet("/mnt/formula1kc/processed/circuits")\
    .withColumnRenamed("name","circuit_name")

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")\
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)
race_circuits_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Outer join

# COMMAND ----------

#Left outer join
truncated_circuits_fd = spark.read.parquet("/mnt/formula1kc/processed/circuits")\
    .filter("circuit_id < 70")\
    .withColumnRenamed("name","circuit_name")

left_outer_join_df = truncated_circuits_fd.join(races_df, truncated_circuits_fd.circuit_id == races_df.circuit_id, "left")\
    .select(truncated_circuits_fd.circuit_name, truncated_circuits_fd.location, truncated_circuits_fd.country, races_df.race_name, races_df.round)
left_outer_join_df.display()

# COMMAND ----------

left_outer_join_df = truncated_circuits_fd.join(races_df, truncated_circuits_fd.circuit_id == races_df.circuit_id, "semi")\
    .select(truncated_circuits_fd.circuit_name, truncated_circuits_fd.location, truncated_circuits_fd.country)

display(left_outer_join_df)

# COMMAND ----------

left_outer_join_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti")\

display(left_outer_join_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Semi Join
# MAGIC - They are very similar to inner joins but only returns the data from left data frame
# MAGIC - With this type of join we cannot select any values from right data frame

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")\
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country).display()

# The below statement will throw an error as we have selected columns from races_df
# race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")\
#     .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.circuit_id).display()


# COMMAND ----------

# MAGIC %md
# MAGIC # Anti join
# MAGIC - Anti join is the opposite of semi join
# MAGIC - Selects everything on left df which is not found on right df

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti").display()

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "anti").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Cross join
# MAGIC - Cross join provides cartition product
# MAGIC - It takes every element in left and matches to every other element on right and gives the result

# COMMAND ----------

cross_product = races_df.crossJoin(circuits_df)
cross_product.count()

# COMMAND ----------

cross_product.display()
