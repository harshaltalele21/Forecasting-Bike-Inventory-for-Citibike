# Databricks notebook source
# MAGIC %run ./includes/includes

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

# Validation
display(spark.sql('select min(started_at) from g04_db.bronze_bike_trip_historic where started_at!="started_at"'))

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
display(spark.sql('CREATE TABLE if not exists silver_bike_trip_historic as select *,hour(started_at) as hourofday_sa, day(started_at) as dateofmonth_sa, dayofyear(started_at) as dateofyear_sa,month(started_at) as monthofyr_sa, year(started_at) as year_sa,hour(ended_at) as hourofday_ea, day(ended_at) as dateofmonth_ea, dayofyear(ended_at) as dateofyear_ea,month(ended_at) as monthofyr_ea, year(ended_at) as year_ea from (select * from bronze_bike_trip_historic where started_at!="started_at")'))

# COMMAND ----------

# Creating silver table for bike trip historic table
display(spark.sql("create table if not exists silver_bike_trip_historic_v1 as select *,date_format(started_at,'EEEE') as weekday_startdate,date_format(ended_at,'EEEE') as weekday_enddate  from silver_bike_trip_historic"))
spark.sql('drop table if exists silver_bike_trip_historic')
spark.sql('create table if not exists silver_bike_trip_historic as select * from silver_bike_trip_historic_v1')
spark.sql('drop table if exists silver_bike_trip_historic_v1')

# COMMAND ----------

# Creating silver table for weather info dynamic table
display(spark.sql("drop table if exists silver_weather_info_dynamic"))
display(spark.sql('create table if not exists silver_weather_info_dynamic as select a.*,final_weather.description as weather_description,final_weather.icon as weather_icon,final_weather.id as weather_id,final_weather.main as weather_main, hour(time) as hourofday, day(time) as dateofmonth, dayofyear(time) as dateofyear,month(time) as monthofyr, year(time) as year from (select *,explode(weather) as final_weather from bronze_weather_info_dynamic) as a '))

# COMMAND ----------

# Creating silver table for weather info dynamic table
display(spark.sql("create table if not exists silver_weather_info_dynamic_v1 as select *,date_format(time,'EEEE') as weekday  from silver_weather_info_dynamic"))
spark.sql('drop table if exists silver_weather_info_dynamic')
spark.sql('create table if not exists silver_weather_info_dynamic as select * from silver_weather_info_dynamic_v1')
spark.sql('drop table if exists silver_weather_info_dynamic_v1')

# COMMAND ----------

# Creating silver table for weather historic table
display(spark.sql("drop table if exists silver_weather_historic"))
display(spark.sql('CREATE TABLE if not exists silver_weather_historic as select a.*,hour(last_reported_datetime) as hourofday, day(last_reported_datetime) as dateofmonth, dayofyear(last_reported_datetime) as dateofyear,month(last_reported_datetime) as monthofyr, year(last_reported_datetime) as year from (select *,to_timestamp(int(dt)) as last_reported_datetime from bronze_weather_historic where dt!="dt") as a'))

# COMMAND ----------

# Creating silver table for weather historic table
display(spark.sql("create table if not exists silver_weather_historic_v1 as select *,date_format(last_reported_datetime,'EEEE') as weekday  from silver_weather_historic"))
spark.sql('drop table if exists silver_weather_historic')
spark.sql('create table if not exists silver_weather_historic as select * from silver_weather_historic_v1')
spark.sql('drop table if exists silver_weather_historic_v1')

# COMMAND ----------

display(spark.sql('select * from silver_weather_historic '))  

# COMMAND ----------

# Checking count of rows in each silver table
display(spark.sql('select "silver_bike_trip_historic" as tablename,count(*) as rows from silver_bike_trip_historic union select "silver_station_status_dynamic" as tablename,count(*) as rows from silver_station_status_dynamic union select "silver_weather_historic" as tablename,count(*) as rows from silver_weather_historic union select "silver_weather_info_dynamic" as tablename,count(*) as rows from silver_weather_info_dynamic'))


# COMMAND ----------

display(spark.sql('drop table if exists target_variable'))
display(spark.sql('create table if not exists target_variable as select a.year_sa,a.monthofyr_sa,a.dateofmonth_sa,a.hourofday_sa,coalesce(a.rides_started,0) as rides_started,coalesce(b.rides_ended,0) as rides_ended,(coalesce(b.rides_ended,0)-coalesce(a.rides_started,0)) as netchange from (select year_sa,monthofyr_sa,dateofmonth_sa,hourofday_sa,count(distinct ride_id) as rides_started from silver_bike_trip_historic where start_station_name="6 Ave & W 33 St"  and started_at<="2023-03-31 23:59:57" and ended_at<="2023-03-31 23:59:57" group by year_sa,monthofyr_sa,dateofmonth_sa,hourofday_sa) as a full outer join (select year_ea,monthofyr_ea,dateofmonth_ea,hourofday_ea,count(distinct ride_id) as rides_ended from silver_bike_trip_historic where end_station_name="6 Ave & W 33 St" and started_at="2023-03-31 23:59:57" and ended_at<="2023-03-31 23:59:57" group by year_ea,monthofyr_ea,dateofmonth_ea,hourofday_ea) as b on a.year_sa=b.year_ea and a.monthofyr_sa=b.monthofyr_ea and a.dateofmonth_sa=b.dateofmonth_ea and a.hourofday_sa=b.hourofday_ea'))  

# COMMAND ----------

display(spark.sql('select * from target_variable limit 5 '))  

# COMMAND ----------

# Validation - Checking target variable data for model training 
display(spark.sql('select * from silver_bike_trip_historic where (start_station_name="6 Ave & W 33 St" or end_station_name="6 Ave & W 33 St") and year_sa=2021 and monthofyr_sa=11 and dateofmonth_sa=1 and (hourofday_sa=0 or hourofday_ea=0) and year_ea=2021 and monthofyr_ea=11 and dateofmonth_ea=1'))

# COMMAND ----------

#Validation - No null records
display(spark.sql('select * from target_variable where netchange IS NULL'))

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
