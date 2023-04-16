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
  .option("complete","true") \
  .csv(BIKE_TRIP_DATA_PATH)

weather_data = spark \
  .readStream \
  .schema(weather_for_schema.schema) \
  .option("maxFilesPerTrigger", 1) \
  .option("complete","true") \
  .csv(NYC_WEATHER_FILE_PATH)

# Load station information and status into DataFrames
station_info_all = spark.read.format("delta").load(BRONZE_STATION_INFO_PATH)
station_status_all = spark.read.format("delta").load(BRONZE_STATION_STATUS_PATH)
weather_dynamic_all = spark.read.format("delta").load(BRONZE_NYC_WEATHER_PATH)


# COMMAND ----------

station_info=station_info_all.filter(station_info_all["name"]==GROUP_STATION_ASSIGNMENT)
display(station_info)

# COMMAND ----------

group_id=station_info.select("station_id").collect()[0][0]

station_status=station_status_all.filter(station_status_all["station_id"]==group_id)

display(station_status)

# COMMAND ----------

display(bike_trip_data)

# COMMAND ----------

#loc=GROUP_DATA_PATH+

#import os
#dbutils.fs.ls("dbfs:/FileStore/tables/")
#station_status.write.format("delta").saveAsTable("station_info_table")
#df_writer = pyspark.sql.DataFrameWriter(station_status)
#df.write.saveAsTable("my_database.my_table")
station_status.write.saveAsTable("G04_db.bronze_station_status_dynamic", format='delta', mode='overwrite')
station_info.write.saveAsTable("G04_db.bronze_station_info_dynamic", format='delta', mode='overwrite')
weather_dynamic_all.write.saveAsTable("G04_db.bronze_weather_info_dynamic", format='delta', mode='overwrite')
#bike_trip_data.write.saveAsTable("G04_db.bronze_bike_trip_historic", format='delta', mode='overwrite')
#weather_data.write.saveAsTable("G04_db.bronze_weather_historic", format='delta', mode='overwrite')

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/G04/"))

# COMMAND ----------

#import os
#os.rmdir("dbfs:/FileStore/tables/G04/sources/")

import shutil

shutil.rmtree("dbfs:/FileStore/tables/G04/")

# COMMAND ----------

spark.sql('use database g04_db')

# COMMAND ----------

display(spark.sql('show tables'))

# COMMAND ----------

spark.sql('select * from g04_db.bronze_bike_trip_historic').show()

# COMMAND ----------

#bike_trip_data.write.saveAsTable("G04_db.bronze_bike_trip_historic", format='delta', mode='overwrite')

#display(type(station_info_all))
#display(type(bike_trip_data))

bike_trip_data.writeStream.format("delta")\
  .outputMode("append")\
  .trigger(once=True)\
  .option("checkpointLocation","dbfs:/FileStore/tables/G04/")\
  .toTable("bronze_bike_trip_historic")

# COMMAND ----------

display(bike_trip_data.count())

# COMMAND ----------

display(BRONZE_STATION_INFO_PATH)


# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
