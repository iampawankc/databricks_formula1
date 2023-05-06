# Databricks notebook source
# MAGIC %md
# MAGIC # Securely accessing ADLS using keyvault and scopes
# MAGIC
# MAGIC 1. Create a scope accessing secrets/createScope
# MAGIC 2. list the scopes create using dbutils.secrets.listScopes()

# COMMAND ----------

display(dbutils.secrets.listScopes())

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.list(scope='formula1-scope')

# COMMAND ----------

dbutils.secrets.get(scope='formula1-scope', key='formula1dl-account-key')

# COMMAND ----------

access_key=dbutils.secrets.get(scope='formula1-scope', key='formula1dl-account-key')

spark.conf.set("fs.azure.account.key.formula1kc.dfs.core.windows.net", access_key)
display(dbutils.fs.ls("abfss://raw@formula1kc.dfs.core.windows.net"))
