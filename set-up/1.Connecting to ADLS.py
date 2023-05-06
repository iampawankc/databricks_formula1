# Databricks notebook source
# MAGIC %md
# MAGIC # 1.Accessing datalake using access keys

# COMMAND ----------

access_key="XA8aPNsMCMHq11GegnvP7jyiayIXkaOpIbxhStDBbu+m8se5bvWScQq2Xx5DJkYxvxakf80qUk73+ASto6qwfg=="

spark.conf.set("fs.azure.account.key.formula1kc.dfs.core.windows.net", access_key)
display(dbutils.fs.ls("abfss://raw@formula1kc.dfs.core.windows.net"))

# COMMAND ----------

circuits_df = spark.read.csv("abfss://raw@formula1kc.dfs.core.windows.net/circuits.csv", header=True)
display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.Access ADLS using SAS token

# COMMAND ----------

sas_token = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-05-07T17:16:53Z&st=2023-05-06T09:16:53Z&spr=https&sig=H91oXW2V%2BQHTJNmAfQjAPBa6LdpeB8jpAdXv%2BbwW5gg%3D"

spark.conf.set("fs.azure.account.auth.type.formula1kc.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1kc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1kc.dfs.core.windows.net", sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@formula1kc.dfs.core.windows.net"))

# COMMAND ----------

circuits_df = spark.read.csv("abfss://raw@formula1kc.dfs.core.windows.net/circuits.csv", header=True)
display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #3.Accessing ADLS via service principal

# COMMAND ----------

client_id="a0af0886-ab42-4f0b-b75b-4fb87edcb656"
tenant_id="1716df7a-fbbb-4c4a-87ce-4fa72b52b4a2"
app_secret="fC18Q~Sw95SrxWgHNjmnd6XcptkP4XGALtjbuatx"

#once the AAD app is created, assign "Storage blob data contributor" role in role assignments to access the data.


# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1kc.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1kc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1kc.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1kc.dfs.core.windows.net", app_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1kc.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@formula1kc.dfs.core.windows.net"))
