# Databricks notebook source
# MAGIC %md
# MAGIC # Get the env ready

# COMMAND ----------

# Import libraries
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import pyspark.sql.functions as F
from pyspark.sql.functions import col
print("Welcome to the W261 final project!")
data_BASE_DIR = "dbfs:/mnt/mids-w261/"

# some helpful resources on how to use dbutils
dbutils.fs.help()

# COMMAND ----------

# Connect to storage
# The following blob storage is accessible to team members only (read and write)
# access key is valid til TTL
# after that you will need to create a new SAS key and authenticate access again via DataBrick command line
blob_container = "261storagecontainer"  # The name of your container created in https://portal.azure.com
storage_account = "261storage"  # The name of your Storage account created in https://portal.azure.com
secret_scope = "261_team_6_1_spring24_scope"  # The name of the scope created in your local computer using the Databricks CLI
secret_key = "team_6_1_key"  # The name of the secret key created in your local computer using the Databricks CLI
team_blob_url = f"wasbs://{blob_container}@{storage_account}.blob.core.windows.net"  # points to the root of your team storage bucket
mids261_mount_path = "/mnt/mids-w261" # the 261 course blob storage is mounted here.

# SAS Token: Grant the team limited access to Azure Storage resources
spark.conf.set(
    f"fs.azure.sas.{blob_container}.{storage_account}.blob.core.windows.net",
    dbutils.secrets.get(scope=secret_scope, key=secret_key),
)

# Example Usage
# Create a Spark dataframe from a pandas DF
#pdf = pd.DataFrame([[1, 2, 3, "Jane"], [2, 2, 2, None], [12, 12, 12, "John"]], columns=["x", "y", "z", "a_string"])
#df = spark.createDataFrame(pdf)  

# The following can write the dataframe to the team's Cloud Storage
# df.write.mode("overwrite").parquet(f"{team_blob_url}/temp")

# see what's in the blob storage root folder
# display(dbutils.fs.ls(f"{team_blob_url}"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Data for Phase 1:
# MAGIC - OTPW Data: This is our joined data (We joined Airlines and Weather). This is the main dataset for your project, the previous 3 are given for reference. You can attempt your own join for Extra Credit. Location `dbfs:/mnt/mids-w261/OTPW_60M/` and more, several samples are given!

# COMMAND ----------

# MAGIC %md
# MAGIC ## OTPW

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load the data

# COMMAND ----------

df_otpw = spark.read.format("csv").option("header", "true").load(f"dbfs:/mnt/mids-w261/OTPW_3M_2015.csv")
display(df_otpw)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EDA

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Check for relationship between OP_CARRIER, DAY_OF_WEEK, and delays
# MAGIC

# COMMAND ----------

carriers_subset_columns = ['OP_CARRIER', 'DAY_OF_WEEK', 'DEP_DELAY_NEW', 'DEP_DEL15', 'ARR_DELAY_NEW', 'ARR_DEL15', 'CARRIER_DELAY', 'WEATHER_DELAY', 'NAS_DELAY', 'SECURITY_DELAY', 'LATE_AIRCRAFT_DELAY']
carriers_subset = df_otpw.select(carriers_subset_columns)

# Convert columns to appropriate data types
columns_to_convert = carriers_subset_columns[1:]
for column in columns_to_convert:
    carriers_subset = carriers_subset.withColumn(f"{column}_float", col(column).cast("float"))

display(carriers_subset)

# COMMAND ----------

carriers_agg = carriers_subset.groupBy(['DAY_OF_WEEK', 'OP_CARRIER']).agg(F.mean("DEP_DEL15_float").alias("Avg_Dep_Del_Count"),
                                                                       F.mean("ARR_DEL15_float").alias("Avg_Arr_Del_Count"),
                                                                       F.mean("DEP_DELAY_NEW_float").alias("Avg_Dep_Del_Mins"),
                                                                        F.mean("ARR_DELAY_NEW_float").alias("Avg_Arr_Del_Mins"),
                                                                        F.mean("CARRIER_DELAY_float").alias("Avg_Car_Del_Mins"),
                                                                        F.mean("WEATHER_DELAY_float").alias("Avg_Wea_Del_Mins"),
                                                                        F.mean("NAS_DELAY_float").alias("Avg_Nas_Del_Mins"),
                                                                        F.mean("SECURITY_DELAY_float").alias("Avg_Sec_Del_Mins"),
                                                                        F.mean("LATE_AIRCRAFT_DELAY_float").alias("Avg_Lat_Del_Mins")).toPandas()

display(carriers_agg)

# COMMAND ----------

# Columns to plot
columns_to_plot = ['Avg_Dep_Del_Count', 'Avg_Arr_Del_Count', 'Avg_Dep_Del_Mins', 'Avg_Arr_Del_Mins',
                   'Avg_Car_Del_Mins', 'Avg_Wea_Del_Mins', 'Avg_Nas_Del_Mins', 'Avg_Sec_Del_Mins', 
                   'Avg_Lat_Del_Mins']

# Number of rows and columns for subplots
num_rows = 2
num_cols = 5

# Create subplots
fig, axes = plt.subplots(num_rows, num_cols, figsize=(20, 10))

# Flatten axes for easier indexing
axes = axes.flatten()

for i, col in enumerate(columns_to_plot):
    pivot_table = carriers_agg.pivot(index='OP_CARRIER', columns='DAY_OF_WEEK', values=col)
    sns.heatmap(pivot_table, cmap='YlGnBu', ax=axes[i])
    axes[i].set_title(f'{col}')
    axes[i].set_xlabel('Day of Week')
    axes[i].set_ylabel('Carrier')

# Hide extra subplots if not needed
for j in range(len(columns_to_plot), num_rows * num_cols):
    fig.delaxes(axes[j])

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Check for relationship between DEP_DELAY_NEW, and DISTANCE + weather

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

numeric_subset_columns = ['DEP_DELAY_NEW', 'DISTANCE', 'HourlyDewPointTemperature', 'HourlyDryBulbTemperature', 'HourlyPrecipitation', 'HourlyVisibility', 'HourlyWetBulbTemperature', 'HourlyWindDirection', 'HourlyWindSpeed']
numeric_subset = df_otpw.select(numeric_subset_columns)

# Convert columns to appropriate data types
for column in numeric_subset_columns:
    numeric_subset = numeric_subset.withColumn(f"{column}_float", F.col(column).cast("float"))

numeric_subset = numeric_subset.filter(F.col('DEP_DELAY_NEW_float')>0.0)

display(numeric_subset)

# COMMAND ----------

numeric_df = numeric_subset.toPandas()
plt.figure(figsize=(20, 20))
sns.pairplot(numeric_df)
plt.show()
