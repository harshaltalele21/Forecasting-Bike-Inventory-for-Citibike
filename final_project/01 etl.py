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
  .option("append","true") \
  .csv(BIKE_TRIP_DATA_PATH)

weather_data = spark \
  .readStream \
  .schema(weather_for_schema.schema) \
  .option("maxFilesPerTrigger", 1) \
  .option("append","true") \
  .csv(NYC_WEATHER_FILE_PATH)


# COMMAND ----------

station_info_all = spark.read.format("delta").load(BRONZE_STATION_INFO_PATH)
station_status_all = spark.read.format("delta").load(BRONZE_STATION_STATUS_PATH)
weather_dynamic_all = spark.read.format("delta").load(BRONZE_NYC_WEATHER_PATH)

# COMMAND ----------

# Creating station info table for our station name
station_info=station_info_all.filter(station_info_all["name"]==GROUP_STATION_ASSIGNMENT)
display(station_info)

# COMMAND ----------

station_info_all.select("station_type").distinct().show()

# COMMAND ----------

# Creating station status table for our station name
group_id=station_info.select("station_id").collect()[0][0]
station_status=station_status_all.filter(station_status_all["station_id"]==group_id)
display(station_status)

# COMMAND ----------

# Creating dynamic tables
station_status.write.saveAsTable("G04_db.bronze_station_status_dynamic", format='delta', mode='overwrite')
station_info.write.saveAsTable("G04_db.bronze_station_info_dynamic", format='delta', mode='overwrite')
weather_dynamic_all.write.partitionBy("time").option("overwriteSchema", "true").saveAsTable("G04_db.bronze_weather_info_dynamic", format='delta', mode='overwrite')

# COMMAND ----------

# Show files under group file path for respective historic data sets
display(dbutils.fs.ls("dbfs:/FileStore/tables/G04/weather_data"))

# COMMAND ----------

# Show files under group file path for respective historic data sets
display(dbutils.fs.ls("dbfs:/FileStore/tables/G04/bike_trip_data"))

# COMMAND ----------

# to remove files from directory
dbutils.fs.rm('dbfs:/FileStore/tables/G04/bike_trip_data/',True)

# COMMAND ----------

spark.sql('use database g04_db')

# COMMAND ----------

# Get tables
display(spark.sql('show tables'))

# COMMAND ----------

# Get tables
display(spark.sql('drop table bronze_weather_info_dynamic'))

# COMMAND ----------

# Validation# Validation
display(spark.sql('select max(started_at) from g04_db.bronze_bike_trip_historic where started_at!="started_at"'))

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

 #Creating silver tables

# COMMAND ----------

# Creating silver table for bike status info dynamic table
display(spark.sql("drop table if exists silver_station_status_dynamic"))
display(spark.sql('CREATE TABLE if not exists silver_station_status_dynamic as select a.*,hour(last_reported_datetime) as hourofday, day(last_reported_datetime) as dateofmonth, dayofyear(last_reported_datetime) as dateofyear,month(last_reported_datetime) as monthofyr, year(last_reported_datetime) as year from (select *,to_timestamp(last_reported) as last_reported_datetime from bronze_station_status_dynamic) as a'))

# COMMAND ----------

# Creating silver table for bike status info dynamic table
display(spark.sql("create table if not exists silver_station_status_dynamic_v1 as select *,date_format(last_reported_datetime,'EEEE') as weekday  from silver_station_status_dynamic"))
spark.sql('drop table if exists silver_station_status_dynamic')
spark.sql('create table if not exists silver_station_status_dynamic as select * from silver_station_status_dynamic_v1')
spark.sql('drop table if exists silver_station_status_dynamic_v1')

# COMMAND ----------

# Creating silver table for bike trip historic table
display(spark.sql("drop table if exists silver_bike_trip_historic"))
display(spark.sql('CREATE TABLE if not exists silver_bike_trip_historic as select *,hour(started_at) as hourofday, day(started_at) as dateofmonth, dayofyear(started_at) as dateofyear,month(started_at) as monthofyr, year(started_at) as year from bronze_bike_trip_historic'))

# COMMAND ----------

# Creating silver table for bike trip historic table
display(spark.sql("create table if not exists silver_bike_trip_historic_v1 as select *,date_format(started_at,'EEEE') as weekday  from silver_bike_trip_historic"))
spark.sql('drop table if exists silver_bike_trip_historic')
spark.sql('create table if not exists silver_bike_trip_historic as select * from silver_bike_trip_historic_v1')
spark.sql('drop table if exists silver_bike_trip_historic_v1')

# COMMAND ----------

# Creating silver table for weather info dynamic table
display(spark.sql("drop table if exists silver_weather_info_dynamic"))
display(spark.sql('create table if not exists silver_weather_info_dynamic as select temp,feels_like,pressure,humidity,dew_point,uvi,clouds,visibility,wind_speed,wind_deg,wind_gust,pop,"rain.1h",final_weather.description as weather_description,final_weather.icon as weather_icon,final_weather.id as weather_id,final_weather.main as weather_main,time, hour(time) as hourofday, day(time) as dateofmonth, dayofyear(time) as dateofyear,month(time) as monthofyr, year(time) as year from (select *,explode(weather) as final_weather from bronze_weather_info_dynamic) as a '))

# COMMAND ----------

# Creating silver table for weather info dynamic table
display(spark.sql("create table if not exists silver_weather_info_dynamic_v1 as select *,date_format(time,'EEEE') as weekday  from silver_weather_info_dynamic"))
spark.sql('drop table if exists silver_weather_info_dynamic')
spark.sql('create table if not exists silver_weather_info_dynamic as select * from silver_weather_info_dynamic_v1')
spark.sql('drop table if exists silver_weather_info_dynamic_v1')

# COMMAND ----------

# Creating silver table for weather historic table
display(spark.sql("drop table if exists silver_weather_historic"))
display(spark.sql('CREATE TABLE if not exists silver_weather_historic as select a.*,hour(last_reported_datetime) as hourofday, day(last_reported_datetime) as dateofmonth, dayofyear(last_reported_datetime) as dateofyear,month(last_reported_datetime) as monthofyr, year(last_reported_datetime) as year from (select *,to_timestamp(int(dt)) as last_reported_datetime from bronze_weather_historic) as a'))

# COMMAND ----------

# Creating silver table for weather historic table
display(spark.sql("create table if not exists silver_weather_historic_v1 as select *,date_format(last_reported_datetime,'EEEE') as weekday  from silver_weather_historic"))
spark.sql('drop table if exists silver_weather_historic')
spark.sql('create table if not exists silver_weather_historic as select * from silver_weather_historic_v1')
spark.sql('drop table if exists silver_weather_historic_v1')

# COMMAND ----------

display(spark.sql('select * from silver_weather_historic'))

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
