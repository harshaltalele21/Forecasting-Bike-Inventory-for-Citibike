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



# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

bike_for_schema = spark.read.csv(BIKE_TRIP_DATA_PATH,sep=",",header="true")
weather_for_schema = spark.read.csv(NYC_WEATHER_FILE_PATH,sep=",",header="true")

# Stream bike trip data into a DataFrame
bike_trip_data = spark \
  .readStream \
  .schema(bike_for_schema.schema) \
  .option("maxFilesPerTrigger", 1) \
  .csv(BIKE_TRIP_DATA_PATH)

weather_data = spark \
  .readStream \
  .schema(weather_for_schema.schema) \
  .option("maxFilesPerTrigger", 1) \
  .csv(NYC_WEATHER_FILE_PATH)

# Load station information and status into DataFrames
station_info_all = spark.read.format("delta").load(BRONZE_STATION_INFO_PATH)
station_status_all = spark.read.format("delta").load(BRONZE_STATION_STATUS_PATH)
weather_dynamic_all = spark.read.format("delta").load(BRONZE_NYC_WEATHER_PATH)


# COMMAND ----------

station_info=station_info_all.filter(station_info_all["name"]==GROUP_STATION_ASSIGNMENT)
display(station_info)

# COMMAND ----------



# COMMAND ----------

# #station_status = station_status_all \
# #  .join(station_info, station_status_all.station_id == station_info.station_id, "inner")
# display(station_status)

station_status=station_status_all.filter(station_status_all["station_id"]==station_info.select("station_id").distinct().collect()[0]["station_id"])

display(station_status)

# COMMAND ----------

#loc=GROUP_DATA_PATH+

#import os
#dbutils.fs.ls("dbfs:/FileStore/tables/")
#station_status.write.format("delta").saveAsTable("station_info_table")
#df_writer = pyspark.sql.DataFrameWriter(station_status)
#df.write.saveAsTable("my_database.my_table")
station_status.write.saveAsTable("G04_db.bronze_station_info", format='delta', mode='overwrite',path=GROUP_DATA_PATH)

# COMMAND ----------

display(dbutils.fs.ls(GROUP_DATA_PATH))

# COMMAND ----------

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

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
