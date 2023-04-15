# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast, promote_model)
print("YOUR CODE HERE...")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define the schema for the bike trip data
bike_trip_schema = StructType([
  StructField("trip_id", StringType()),
  StructField("start_time", StringType()),
  StructField("end_time", StringType()),
  StructField("bike_id", StringType()),
  StructField("duration_sec", IntegerType()),
  StructField("start_station_name", StringType()),
  StructField("start_station_id", IntegerType()),
  StructField("end_station_name", StringType()),
  StructField("end_station_id", IntegerType()),
  StructField("user_type", StringType()),
  StructField("bike_share_for_all_trip", StringType())
])

# Stream bike trip data into a DataFrame
bike_trip_data = spark \
  .readStream \
  .schema(bike_trip_schema) \
  .option("maxFilesPerTrigger", 1) \
  .csv(BIKE_TRIP_DATA_PATH)

# Load station information and status into DataFrames
station_info = spark.read.format("delta").load(BRONZE_STATION_INFO_PATH)
station_status = spark.read.format("delta").load(BRONZE_STATION_STATUS_PATH)

# Join station information and status with the bike trip data
station_data = bike_trip_data \
  .join(station_info, bike_trip_data.start_station_id == station_info.station_id, "left_outer") \
  .join(station_status, bike_trip_data.start_station_id == station_status.station_id, "left_outer") \
  .select(
    bike_trip_data.trip_id,
    bike_trip_data.start_time,
    bike_trip_data.end_time,
    bike_trip_data.duration_sec,
    bike_trip_data.start_station_name,
    bike_trip_data.start_station_id,
    station_info.name.alias("start_station_info"),
    station_status.num_bikes_available.alias("start_station_bikes_available"),
    station_status.num_docks_available.alias("start_station_docks_available")
  )

# Display the streaming station data
display(station_data)

# COMMAND ----------



# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
