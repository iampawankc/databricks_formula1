# Databricks notebook source
# MAGIC %md
# MAGIC # Securely mounting ADLS to databricks
# MAGIC 1. Create AAD application and fetch the ClientID, TenentID and secret
# MAGIC 2. Create a scope in databricks by accessing => secrets/createScope
# MAGIC 3. Optionally, list the scopes using dbutils.secrets.listScopes()
# MAGIC 4. Call the file system utility mount to moubt the storage.
# MAGIC 5. Explaore the data using the mount (list all mounts, unmount, browse through data)

# COMMAND ----------

display(dbutils.secrets.listScopes())

# COMMAND ----------

display(dbutils.secrets.list(scope="formula1-scope"))

# COMMAND ----------

client_id=dbutils.secrets.get(scope="formula1-scope", key="clientid")
tenant_id=dbutils.secrets.get(scope="formula1-scope", key="tenantid")
app_secret=dbutils.secrets.get(scope="formula1-scope", key="appsecret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": app_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://raw@formula1kc.dfs.core.windows.net/",
  mount_point = "/mnt/formula1kc/raw",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://processed@formula1kc.dfs.core.windows.net/",
  mount_point = "/mnt/formula1kc/processed",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://presentation@formula1kc.dfs.core.windows.net/",
  mount_point = "/mnt/formula1kc/presentation",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1kc/raw"))

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/airlines"))

# COMMAND ----------

airline_df = spark.read.csv("dbfs:/databricks-datasets/flights/departuredelays.csv")

# COMMAND ----------

f = open("/dbfs/databricks-datasets/airlines/README.md", 'r')
print(f.read())
