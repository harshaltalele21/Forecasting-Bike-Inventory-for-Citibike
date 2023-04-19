# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

#print(start_date,end_date,hours_to_forecast, promote_model)
#print("YOUR CODE HERE...")


# COMMAND ----------


from pyspark.sql.functions import *
from pyspark.sql.types import *

bike_for_schema = spark.read.csv(BIKE_TRIP_DATA_PATH,sep=",",header="true")
weather_for_schema = spark.read.csv(NYC_WEATHER_FILE_PATH,sep=",",header="true")


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

station_info_all = spark.read.format("delta").load(BRONZE_STATION_INFO_PATH)
station_status_all = spark.read.format("delta").load(BRONZE_STATION_STATUS_PATH)
weather_dynamic_all = spark.read.format("delta").load(BRONZE_NYC_WEATHER_PATH)


# COMMAND ----------

# Creating station info table for our station name
station_info=station_info_all.filter(station_info_all["name"]==GROUP_STATION_ASSIGNMENT)
display(station_info)

# COMMAND ----------

GROUP_STATION_ASSIGNMENT

# COMMAND ----------

# Creating station status table for our station name
group_id=station_info.select("station_id").collect()[0][0]
station_status=station_status_all.filter(station_status_all["station_id"]==group_id)
display(station_status)

# COMMAND ----------

# Creating dynamic tables
station_status.write.saveAsTable("G04_db.bronze_station_status_dynamic", format='delta', mode='overwrite')
station_info.write.saveAsTable("G04_db.bronze_station_info_dynamic", format='delta', mode='overwrite')
weather_dynamic_all.write.saveAsTable("G04_db.bronze_weather_info_dynamic", format='delta', mode='overwrite')

# COMMAND ----------

# Show files under group file path for respective historic data sets
display(dbutils.fs.ls("dbfs:/FileStore/tables/G04/weather_data"))

# COMMAND ----------

# to remove files from directory
dbutils.fs.rm('dbfs:/FileStore/tables/G04/bike_trip_dataweather_data/',True)

# COMMAND ----------

spark.sql('use database g04_db')

# COMMAND ----------

# Get tables
display(spark.sql('show tables'))

# COMMAND ----------

# Validation
display(spark.sql('select * from g04_db.bronze_bike_trip_historic where (end_station_name=="6 Ave & W 33 St") or \
(start_station_name=="6 Ave & W 33 St")').limit(5))

# COMMAND ----------

import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar as calendar

dr = pd.date_range(start='2021-11-01', end='2023-02-28')
df = pd.DataFrame()
df['Date'] = dr

cal = calendar()
holidays = cal.holidays(start=dr.min(), end=dr.max())

df['Date']=pd.to_datetime(df["Date"]).dt.date
df['Holiday'] = df['Date'].isin(holidays)
display(df)

# COMMAND ----------

# Validation# Validation
display(spark.sql('select max(started_at) from g04_db.bronze_bike_trip_historic where started_at!="started_at"'))# Validation# Validation
display(spark.sql('select max(started_at) from g04_db.bronze_bike_trip_historic where started_at!="started_at"'))

# COMMAND ----------

# Validation
display(spark.sql('select count(*) from g04_db.bronze_weather_historic'))

# COMMAND ----------

# Write Strean to append bike trips data

bike_trip_data.writeStream.format("delta")\
  .outputMode("append")\
  .option("checkpointLocation","dbfs:/FileStore/tables/G04/bike_trip_data/checkpoint")\
  .start("dbfs:/FileStore/tables/G04/bike_trip_data/")


# COMMAND ----------

# Creating bike trips historic table
bike_stream = spark.read.format("delta").load("dbfs:/FileStore/tables/G04/bike_trip_data/")
bike_stream.write.format("delta").mode("overwrite").saveAsTable("g04_db.bronze_bike_trip_historic")

# COMMAND ----------

# Write Strean to append weather data

weather_data.writeStream.format("delta")\
  .outputMode("append")\
  .option("checkpointLocation","dbfs:/FileStore/tables/G04/weather_data/checkpoint")\
  .start("dbfs:/FileStore/tables/G04/weather_data/")


# COMMAND ----------

# Creating weather historic table
weather_stream = spark.read.format("delta").load("dbfs:/FileStore/tables/G04/weather_data/")
weather_stream.write.format("delta").mode("overwrite").saveAsTable("g04_db.bronze_weather_historic")

# COMMAND ----------

display(bike_stream.count())


# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
