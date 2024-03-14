# Databricks notebook source
# MAGIC %md
# MAGIC # Get the env ready

# COMMAND ----------

# Import libraries
import pandas as pd

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
# MAGIC # Data for the Project
# MAGIC
# MAGIC For the project you will have 4 sources of data:
# MAGIC
# MAGIC 1. Airlines Data: This is the raw data of flights information. You have 3 months, 6 months, 1 year, and full data from 2015 to 2019. Remember the maxima: "Test, Test, Test", so a lot of testing in smaller samples before scaling up! Location of the data? `dbfs:/mnt/mids-w261/datasets_final_project_2022/parquet_airlines_data/`, `dbfs:/mnt/mids-w261/datasets_final_project_2022/parquet_airlines_data_1y/`, etc. (Below the dbutils to get the folders)
# MAGIC 2. Weather Data: Raw data for weather information. Same as before, we are sharing 3 months, 6 months, 1 year
# MAGIC 3. Stations data: Extra information of the location of the different weather stations. Location `dbfs:/mnt/mids-w261/datasets_final_project_2022/stations_data/stations_with_neighbors.parquet/`
# MAGIC 4. OTPW Data: This is our joined data (We joined Airlines and Weather). This is the main dataset for your project, the previous 3 are given for reference. You can attempt your own join for Extra Credit. Location `dbfs:/mnt/mids-w261/OTPW_60M/` and more, several samples are given!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Take a look at all the directories under the parent directory

# COMMAND ----------

def list_subdirectories(directory_path, indent=0):
    # List files and directories in the current directory
    files_and_dirs = dbutils.fs.ls(directory_path)

    # Print files and directories
    for item in files_and_dirs:
        if item.isDir():
            print(" " * indent + "+-- " + item.name)
            list_subdirectories(item.path, indent + 4)

list_subdirectories(data_BASE_DIR)


# COMMAND ----------

display(dbutils.fs.ls(f"{data_BASE_DIR}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Airline Data
# MAGIC
# MAGIC Insights:
# MAGIC - Number of rows: 2,806,942
# MAGIC - Number of columns: 109
# MAGIC - Data year: 2015
# MAGIC - Data months: Jan, Feb, Mar
# MAGIC - Number of carriers: 14
# MAGIC - Number of airports: 315
# MAGIC - Delay indicator columns:
# MAGIC     - `DEP_DEL15` (Departure delayed for more than 15 mins)
# MAGIC         - 0 for On-Time: 20%
# MAGIC         - 1 for Delayed: 77%
# MAGIC         - NULL for Cancelled: 3%
# MAGIC     - `ARR_DEL15` (Arrival delayed for more than 15 mins)
# MAGIC         - 0 for On-Time: 20%
# MAGIC         - 1 for Delayed: 76%
# MAGIC         - NULL for Cancelled or Diverted: 4%
# MAGIC - Additional delay info columns:
# MAGIC     - `DEP_DELAY_NEW` (Departure delay in mins. Early departures==0.)
# MAGIC     - `ARR_DELAY_NEW` (Arrival delay in mins. Early departures==0.)
# MAGIC     - `CARRIER_DELAY` (Carrier delay in mins)
# MAGIC     - `WEATHER_DELAY` (Weather delay in mins)
# MAGIC     - `NAS_DELAY` (National Air System delay in mins)
# MAGIC     - `SECURITY_DELAY` (Security delay in mins)
# MAGIC     - `LATE_AIRCRAFT_DELAY` (Late aircraft delay in mins)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overview

# COMMAND ----------

df_flights = spark.read.parquet(f"dbfs:/mnt/mids-w261/datasets_final_project_2022/parquet_airlines_data_3m/")
display(df_flights)

# COMMAND ----------

print(f"Number of rows is {df_flights.count()} and number of columns is {len(df_flights.columns)}")

# COMMAND ----------

flight_data_columns = df_flights.columns
display('percentage of NULL values in each column')
flight_null = {}
for column in flight_data_columns:
    flight_null[column] = round(df_flights.filter(col(column).isNull()).count() / df_flights.count(),2)
{k: v for k, v in sorted(flight_null.items(), key=lambda item: item[1])}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Take a look at the unique values in a few columns

# COMMAND ----------

flight_columns = ['YEAR', 'MONTH', 'OP_UNIQUE_CARRIER', 'TAIL_NUM', 'OP_CARRIER_FL_NUM', 'ORIGIN', 'DEST']
df_flights.select(flight_columns).show(5)
for column in flight_columns:
    display(df_flights.select(column).distinct().count())
    df_flights.select(column).distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Take a look at the flight delay columns

# COMMAND ----------

delay_columns = ['DEP_DEL15', 'ARR_DEL15']
df_flights.select(delay_columns).show(5)
for column in delay_columns:
    display('number of unique values:')
    display(df_flights.select(column).distinct().count())
    df_flights.select(column).distinct().show()
    display('percentage of delayed flights:')
    display(round(df_flights.filter(col(column) == 1.0).count() / df_flights.count(), 2))
    display('percentage of on-time flights:')
    display(round(df_flights.filter(col(column) == 0.0).count() / df_flights.count(), 2))

# COMMAND ----------

delay_details = ['DEP_DELAY_NEW', 'ARR_DELAY_NEW', 'CARRIER_DELAY', 'WEATHER_DELAY', 'NAS_DELAY', 'SECURITY_DELAY', 'LATE_AIRCRAFT_DELAY']
df_flights.select(delay_details).show(5)
df_flights.select(delay_details).describe().show()

# COMMAND ----------

# The 5th row shows NULL in delay indicator column, let's investigate what NULL means
df_flights.limit(5).collect()[4]
# Looks like the flight was cancelled. Let's next find out if all rows with NULL in delay indicator columns are cancelled or diverted flights.

# COMMAND ----------

cancelled = df_flights.filter(col('CANCELLED')==1.0)
diverted = df_flights.filter(col('DIVERTED')==1.0)
dep_delayed_null = df_flights.filter(col('DEP_DEL15').isNull())
arr_delayed_null = df_flights.filter(col('ARR_DEL15').isNull())

display('number of cancelled flights')
display(cancelled.count())
display('number of diverted flights')
display(diverted.count())
display('number of null instances in DEP_DEL15')
display(dep_delayed_null.count())
display('number of null instances in ARR_DEL15')
display(arr_delayed_null.count())

# COMMAND ----------

# asset that all flights with NULL in DEP_DEL15 are cancelled
assert dep_delayed_null.subtract(cancelled).count() == 0

# COMMAND ----------

# asset that all flights with NULL in ARR_DEL15 are cancelled or diverted
assert arr_delayed_null.subtract(cancelled).subtract(diverted).count() == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weather Data
# MAGIC
# MAGIC Insights:
# MAGIC - Number of rows: 30,528,602
# MAGIC - Number of columns: 124
# MAGIC - Data year: 2015
# MAGIC - Number of stations: 12559
# MAGIC - Number of cities: 12370
# MAGIC - Number of report types: 13

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overview

# COMMAND ----------

df_weather = spark.read.parquet(f"dbfs:/mnt/mids-w261/datasets_final_project_2022/parquet_weather_data_3m/")
display(df_weather)

# COMMAND ----------

print(f"Number of rows is {df_weather.count()} and number of columns is {len(df_weather.columns)}")

# COMMAND ----------

weather_data_columns = df_weather.columns
display('percentage of NULL values in each column')
weather_null = {}
for column in weather_data_columns:
    weather_null[column] = round(df_weather.filter(col(column).isNull()).count() / df_weather.count(),2)
{k: v for k, v in sorted(weather_null.items(), key=lambda item: item[1])}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Take a look at the unique values in a few columns

# COMMAND ----------

weather_columns = ['YEAR', 'STATION', 'NAME', 'REPORT_TYPE']
df_weather.select(weather_columns).show(5)
for column in weather_columns:
    display(df_weather.select(column).distinct().count())
    df_weather.select(column).distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Take a look at a few numeric columns

# COMMAND ----------

weather_data_columns = ['HourlyDryBulbTemperature', 'HourlyWindSpeed', 'HourlyWindDirection', 'HourlyDewPointTemperature', 'HourlyRelativeHumidity', 'HourlyVisibility']
df_weather.select(weather_data_columns).show(5)
df_weather.select(weather_data_columns).describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stations data
# MAGIC
# MAGIC Insights:
# MAGIC - Number of rows: 5,004,169
# MAGIC - Number of columns: 12
# MAGIC - No NULL values in any columns
# MAGIC - Number of stations: 2237, one in each neighbor
# MAGIC - Number of states: 52
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overview

# COMMAND ----------

df_stations = spark.read.parquet(f"dbfs:/mnt/mids-w261/datasets_final_project_2022/stations_data/stations_with_neighbors.parquet/")
display(df_stations)

# COMMAND ----------

print(f"Number of rows is {df_stations.count()} and number of columns is {len(df_stations.columns)}")

# COMMAND ----------

station_data_columns = df_stations.columns
display('percentage of NULL values in each column')
station_null = {}
for column in station_data_columns:
    station_null[column] = round(df_stations.filter(col(column).isNull()).count() / df_stations.count(),2)
{k: v for k, v in sorted(station_null.items(), key=lambda item: item[1])}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Take a look at the unique values in a few columns

# COMMAND ----------

station_columns = ['station_id', 'neighbor_id', 'neighbor_state']
df_stations.select(station_columns).show(5)
for column in station_columns:
    display(df_stations.select(column).distinct().count())
    df_stations.select(column).distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Take a look at distance to neighbor

# COMMAND ----------

df_stations.select('distance_to_neighbor').show(5)
df_stations.select('distance_to_neighbor').describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## OTPW
# MAGIC
# MAGIC Insights:
# MAGIC - Number of rows: 1,401,363
# MAGIC - Number of columns: 216
# MAGIC - Flight data joined with airport codes data to retrieve departure and arrival airport information
# MAGIC - Flight data (with airport info) left join with weather data on departure station (QUESTION: SHOULD WE CONSIDER THE WEATHER FOR DESTINATION STATION TOO?)
# MAGIC - Did not check for distribution of NULL values, taking too long
# MAGIC - Data year: 2015
# MAGIC - Data months: Jan, Feb, Mar
# MAGIC - Number of carriers: 14
# MAGIC - Number of departure airports: 313
# MAGIC - Number of arrival airports: 315
# MAGIC - Number of weather stations: 312
# MAGIC - Number of report types: 6
# MAGIC - Delay indicator columns: 
# MAGIC     - DEP_DEL15 (Departure delayed for more than 15 mins)
# MAGIC         - 0 for On-Time: 20%
# MAGIC         - 1 for Delayed: 77%
# MAGIC         - NULL for Cancelled: 3%
# MAGIC     - ARR_DEL15 (Arrival delayed for more than 15 mins)
# MAGIC         - 0 for On-Time: 20%
# MAGIC         - 1 for Delayed: 76%
# MAGIC         - NULL for Cancelled or Diverted: 4%
# MAGIC - Additional delay info columns:
# MAGIC     - DEP_DELAY_NEW (Departure delay in mins. Early departures==0.)
# MAGIC     - ARR_DELAY_NEW (Arrival delay in mins. Early departures==0.)
# MAGIC     - CARRIER_DELAY (Carrier delay in mins)
# MAGIC     - WEATHER_DELAY (Weather delay in mins)
# MAGIC     - NAS_DELAY (National Air System delay in mins)
# MAGIC     - SECURITY_DELAY (Security delay in mins)
# MAGIC     - LATE_AIRCRAFT_DELAY (Late aircraft delay in mins)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overview

# COMMAND ----------

df_otpw = spark.read.format("csv").option("header", "true").load(f"dbfs:/mnt/mids-w261/OTPW_3M_2015.csv")
display(df_otpw)

# COMMAND ----------

print(f"Number of rows is {df_otpw.count()} and number of columns is {len(df_otpw.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Take a look at the unique values in a few columns

# COMMAND ----------

otpw_columns = ['YEAR', 'MONTH', 'OP_UNIQUE_CARRIER', 'ORIGIN', 'DEST', 'STATION', 'REPORT_TYPE']
df_otpw.select(otpw_columns).show(5)
for column in otpw_columns:
    display(df_otpw.select(column).distinct().count())
    df_otpw.select(column).distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Take a look at the flight delay columns

# COMMAND ----------

delay_columns = ['DEP_DEL15', 'ARR_DEL15']
df_otpw.select(delay_columns).show(5)
for column in delay_columns:
    display('number of unique values:')
    display(df_otpw.select(column).distinct().count())
    df_otpw.select(column).distinct().show()
    display('percentage of delayed flights:')
    display(round(df_otpw.filter(col(column) == 1.0).count() / df_otpw.count(), 2))
    display('percentage of on-time flights:')
    display(round(df_otpw.filter(col(column) == 0.0).count() / df_otpw.count(), 2))

# COMMAND ----------

delay_details = ['DEP_DELAY_NEW', 'ARR_DELAY_NEW', 'CARRIER_DELAY', 'WEATHER_DELAY', 'NAS_DELAY', 'SECURITY_DELAY', 'LATE_AIRCRAFT_DELAY']
df_otpw.select(delay_details).show(5)
df_otpw.select(delay_details).describe().show()

# COMMAND ----------

cancelled = df_otpw.filter(col('CANCELLED')==1.0)
diverted = df_otpw.filter(col('DIVERTED')==1.0)
dep_delayed_null = df_otpw.filter(col('DEP_DEL15').isNull())
arr_delayed_null = df_otpw.filter(col('ARR_DEL15').isNull())

# asset that all flights with NULL in DEP_DEL15 are cancelled
assert dep_delayed_null.subtract(cancelled).count() == 0

# asset that all flights with NULL in ARR_DEL15 are cancelled or diverted
assert arr_delayed_null.subtract(cancelled).subtract(diverted).count() == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Take a look at a few weather columns

# COMMAND ----------

weather_data_columns = ['HourlyDryBulbTemperature', 'HourlyWindSpeed', 'HourlyWindDirection', 'HourlyDewPointTemperature', 'HourlyRelativeHumidity', 'HourlyVisibility']
df_otpw.select(weather_data_columns).show(5)
df_otpw.select(weather_data_columns).describe().show()
