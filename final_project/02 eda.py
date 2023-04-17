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

from pyspark.sql.functions import expr,col,month
from pyspark.sql import SparkSession

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

df = spark.read.csv("dbfs:/FileStore/tables/raw/weather/", header=True, inferSchema=True)

# COMMAND ----------

df.count()

# COMMAND ----------

display(bike_trip_data)

# COMMAND ----------

df=bike_trip_data

# COMMAND ----------

merge_df = bike_trip_data.withColumnRenamed("started_at", "datetime")

# COMMAND ----------

display(merge_df)

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/tables/raw/bike_trips/", header=True, inferSchema=True)

# COMMAND ----------

col = df.select("start_station_name")

# COMMAND ----------

new_df = df.filter((df.start_station_name == "6 Ave & W 33 St")|(df.end_station_name=="6 Ave & W 33 St"))

# COMMAND ----------

display(new_df)

# COMMAND ----------

type(new_df)

# COMMAND ----------

selected_df = new_df.select("ride_id", "started_at", "ended_at", "start_station_name", "start_station_id") \
                .withColumn("year", year(col("started_at"))) \
                .withColumn("month", month(col("started_at")))


# COMMAND ----------

agg_df_month = selected_df.groupBy("year","month")\
                          .agg({"ride_id":"count"})\
                          .withColumnRenamed("count(ride_id)","num_rides")\
                          .orderBy("year","month")

# COMMAND ----------

# show the aggregated DataFrame
agg_df_month.show()

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

# read data into a DataFrame
# add index column
agg_df_month_with_index = agg_df_month.withColumn("index", monotonically_increasing_id())

# show the new DataFrame
agg_df_month_with_index.show()

# COMMAND ----------

import matplotlib.pyplot as plt

pandas_df = agg_df_month_with_index.toPandas()

# plot the data using matplotlib
plt.plot(pandas_df["index"], pandas_df["num_rides"])
plt.xlabel("Month")
plt.ylabel("Number of Rides")
plt.title("Monthly Bike Rides")
plt.show()

# COMMAND ----------



# COMMAND ----------

selected_df_member = new_df.select("started_at", "ended_at","member_casual") \
                .withColumn("year", year(col("started_at"))) \
                .withColumn("month", month(col("started_at"))) \
                .withColumn("index", monotonically_increasing_id())

# COMMAND ----------

display(selected_df_member)

# COMMAND ----------

import matplotlib.pyplot as plt

# create a DataFrame with aggregated data
agg_data = selected_df_member.groupBy("year", "month", "member_casual").count()

# filter for member_casual data
member_data = agg_data.filter(agg_data.member_casual == "member").orderBy("year", "month")

# filter for casual data
casual_data = agg_data.filter(agg_data.member_casual == "casual").orderBy("year", "month")

# create line plots
plt.plot(member_data.select("month").collect(), member_data.select("count").collect(), label="member")
plt.plot(casual_data.select("month").collect(), casual_data.select("count").collect(), label="casual")

# add labels and legend
plt.xlabel("Month")
plt.ylabel("Count")
plt.title("Member vs. Casual Ridership")
plt.legend()

# show the plot
plt.show()

# COMMAND ----------

import matplotlib.pyplot as plt

# COMMAND ----------

# create a DataFrame with aggregated data
agg_data = selected_df_member.groupBy("index", "member_casual").count()

# filter for member_casual data
member_data = agg_data.filter(agg_data.member_casual == "member").orderBy("index")

# filter for casual data
casual_data = agg_data.filter(agg_data.member_casual == "casual").orderBy("index")

# create line plots
plt.plot(member_data.select("index").collect(), member_data.select("count").collect(), label="member")
plt.plot(casual_data.select("index").collect(), casual_data.select("count").collect(), label="casual")

# add labels and legend
plt.xlabel("Month")
plt.ylabel("Count")
plt.title("Member vs. Casual Ridership")
plt.legend()

# show the plot
plt.show()

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Scatter Plot
scatter_df = new_df.select("start_station_name")

# COMMAND ----------

#scatter_df = scatter_df.groupBy("start_station_name")

# COMMAND ----------

scatter_df.show()

# COMMAND ----------

import pandas as pd

# COMMAND ----------

pd_scatter = scatter_df.toPandas()

# COMMAND ----------

pd_scatter.plot.scatter(x="start_station_name" , y="end_station_name")
plt.show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Weather Data
df = spark.sql('select * from g04_db.bronze_weather_historic')

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import from_unixtime
df_time = df.withColumn('datetime', from_unixtime('dt'))

# COMMAND ----------

display(df_time)

# COMMAND ----------

df_new = df_time.select("lat") \
                .withColumn("year", year(col("datetime"))) \
                .withColumn("month", month(col("datetime")))

# COMMAND ----------



# COMMAND ----------

new_df = df_time.select("temp","datetime")

# COMMAND ----------

display(new_df)

# COMMAND ----------

pdf = new_df.toPandas()

# COMMAND ----------

plt.plot(pdf['datetime'], pdf['temp'])
plt.xlabel('Month')
plt.ylabel('Average Temperature')
plt.show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import month

# assuming your DataFrame is named "df"
df_avg_temp = new_df.select(month('datetime').alias('month'), 'temp') \
                .groupBy('month') \
                .agg({'temp': 'avg'}) \
                .orderBy('month')

display(df_avg_temp)

# COMMAND ----------

df_inner_join = merge_df.join(df_time, on='datetime', how='inner')

# COMMAND ----------

display(df_inner_join)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))

# COMMAND ----------

display(spark.sql('show tables'))

# COMMAND ----------

from pyspark.sql.functions import from_unixtime

data = df.withColumn('datetime', from_unixtime('dt'))

# COMMAND ----------

display(data)

# COMMAND ----------


