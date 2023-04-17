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

from pyspark.sql.functions import expr,col,month,year,dayofmonth
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws


# COMMAND ----------

raw_df = spark.read.csv("dbfs:/FileStore/tables/raw/bike_trips/", header=True, inferSchema=True)
raw_df = raw_df.filter((raw_df.start_station_name == GROUP_STATION_ASSIGNMENT)|(raw_df.end_station_name == GROUP_STATION_ASSIGNMENT))

# COMMAND ----------

display(raw_df)

# COMMAND ----------

df = raw_df.select("ride_id", "rideable_type", "started_at", "ended_at", "start_station_name", "start_station_id", "end_station_name", "end_station_id" ,"member_casual") \
                .withColumn("year", year(col("started_at"))) \
                .withColumn("month", month(col("started_at"))) \
                .withColumn("day", dayofmonth(col("started_at")))


# COMMAND ----------

agg_df = df.groupBy("year","month","day","rideable_type","member_casual")\
                          .agg({"ride_id":"count"})\
                          .withColumnRenamed("count(ride_id)","num_rides")\
                          .orderBy("year","month","day")
agg_df = agg_df.withColumn("year_month", concat_ws("_","year","month"))
agg_df = agg_df.withColumn("year_month_day", concat_ws("_","year_month","day"))
agg_df.show(10)

# COMMAND ----------

agg_df_month = agg_df.groupBy("year","month")\
                          .agg({"num_rides":"sum"})\
                          .withColumnRenamed("sum(num_rides)","num_rides")\
                          .orderBy("year","month")
agg_df_month = agg_df_month.withColumn("year_month", concat_ws("_","year","month"))
agg_df_month.show()

# COMMAND ----------

agg_df_day = agg_df.groupBy("year","month","day","year_month_day")\
                          .agg({"num_rides":"sum"})\
                          .withColumnRenamed("sum(num_rides)","num_rides")\
                          .orderBy("year","month","day","year_month_day")
agg_df_day.show(10)

# COMMAND ----------

import matplotlib.pyplot as plt

pandas_df = agg_df_month.toPandas()

# plot the data using matplotlib
plt.plot(pandas_df["year_month"], pandas_df["num_rides"])
plt.xlabel("month")
plt.xticks(rotation = 90)
plt.ylabel("Number of Rides")
plt.title("Monthly Bike Rides")
plt.show()

# COMMAND ----------

import matplotlib.pyplot as plt
from pyspark.sql.functions import collect_list

# create a DataFrame with aggregated data
agg_data = agg_df.groupBy("year", "month", "year_month", "member_casual").agg({"num_rides":"sum"}).withColumnRenamed("sum(num_rides)","num_rides")

# filter for member_casual data
member_data = agg_data.filter(agg_df.member_casual == "member").orderBy("year_month")

# filter for casual data
casual_data = agg_data.filter(agg_df.member_casual == "casual").orderBy("year_month")

year_month = [str(row.year_month) for row in member_data.select("year_month").collect()]
member_num_rides = [int(row.num_rides) for row in member_data.select("num_rides").collect()]
casual_num_rides = [int(row.num_rides) for row in casual_data.select("num_rides").collect()]

plt.plot(year_month, member_num_rides, label="member")
plt.plot(year_month, casual_num_rides, label="casual")

# add labels and legend
plt.xlabel("Month")
plt.ylabel("Count")
plt.xticks(rotation = 90)
plt.title("Member vs. Casual Ridership")
plt.legend()

# show the plot
plt.show()

# COMMAND ----------

import matplotlib.pyplot as plt
from pyspark.sql.functions import collect_list

# create a DataFrame with aggregated data
agg_data = agg_df.groupBy("year", "month", "year_month", "rideable_type").agg({"num_rides":"sum"}).withColumnRenamed("sum(num_rides)","num_rides")

# filter for member_casual data
classic_data = agg_data.filter(agg_df.rideable_type == "classic_bike").orderBy("year_month")
electric_data = agg_data.filter(agg_df.rideable_type == "electric_bike").orderBy("year_month")
docked_data = agg_data.filter(agg_df.rideable_type == "docked_bike").orderBy("year_month")


year_month = [str(row.year_month) for row in classic_data.select("year_month").collect()]
classic_num_rides = [int(row.num_rides) for row in classic_data.select("num_rides").collect()]
electric_num_rides = [int(row.num_rides) for row in electric_data.select("num_rides").collect()]
docked_num_rides = [int(row.num_rides) for row in docked_data.select("num_rides").collect()]


plt.plot(year_month, classic_num_rides, label="classic")
plt.plot(year_month, electric_num_rides, label="electric")
plt.plot(year_month, docked_num_rides, label="dock")


# add labels and legend
plt.xlabel("Month")
plt.ylabel("Count")
plt.xticks(rotation = 90)
plt.title("Classic vs. Electric vs. Dock Bike")
plt.legend()

# show the plot
plt.show()

# COMMAND ----------

pandas_df_day = agg_df_day.toPandas()

# plot the data using matplotlib
plt.plot(pandas_df_day["year_month_day"], pandas_df_day["num_rides"])
plt.xlabel("day")
plt.xticks(rotation = 90)
plt.ylabel("Number of Rides")
plt.title("Daily Bike Rides")
plt.show()

# COMMAND ----------

# create a DataFrame with aggregated data
agg_data = agg_df.groupBy("year", "month", "day", "year_month_day", "member_casual").agg({"num_rides":"sum"}).withColumnRenamed("sum(num_rides)","num_rides")

# filter for member_casual data
member_data = agg_data.filter(agg_df.member_casual == "member").orderBy("year_month_day")

# filter for casual data
casual_data = agg_data.filter(agg_df.member_casual == "casual").orderBy("year_month_day")

year_month_day = [str(row.year_month_day) for row in member_data.select("year_month_day").collect()]
member_num_rides = [int(row.num_rides) for row in member_data.select("num_rides").collect()]
casual_num_rides = [int(row.num_rides) for row in casual_data.select("num_rides").collect()]

plt.plot(year_month_day, member_num_rides, label="member")
plt.plot(year_month_day, casual_num_rides, label="casual")

# add labels and legend
plt.xlabel("Month")
plt.ylabel("Count")
plt.xticks(rotation = 90)
plt.title("Member vs. Casual Ridership")
plt.legend()

# show the plot
plt.show()

# COMMAND ----------

import matplotlib.pyplot as plt
from pyspark.sql.functions import collect_list

# create a DataFrame with aggregated data
agg_data = agg_df.groupBy("year", "month", "day", "year_month_day", "rideable_type").agg({"num_rides":"sum"}).withColumnRenamed("sum(num_rides)","num_rides")

# filter for member_casual data
classic_data = agg_data.filter(agg_df.rideable_type == "classic_bike").orderBy("year_month_day")
electric_data = agg_data.filter(agg_df.rideable_type == "electric_bike").orderBy("year_month_day")
docked_data = agg_data.filter(agg_df.rideable_type == "docked_bike").orderBy("year_month_day")


year_month = [str(row.year_month_day) for row in classic_data.select("year_month_day").collect()]
classic_num_rides = [int(row.num_rides) for row in classic_data.select("num_rides").collect()]
electric_num_rides = [int(row.num_rides) for row in electric_data.select("num_rides").collect()]
docked_num_rides = [int(row.num_rides) for row in docked_data.select("num_rides").collect()]


plt.plot(year_month_day, classic_num_rides, label="classic")
plt.plot(year_month_day, electric_num_rides, label="electric")
plt.plot(year_month_day, docked_num_rides, label="dock")


# add labels and legend
plt.xlabel("Day")
plt.ylabel("Count")
plt.xticks(rotation = 90)
plt.title("Classic vs. Electric vs. Dock Bike")
plt.legend()

# show the plot
plt.show()

# COMMAND ----------

# Q3
import pandas as pd
from pyspark.sql.functions import *
from pandas.tseries.holiday import USFederalHolidayCalendar as calendar

dates = pd.DataFrame({'datetime':pd.date_range('2021-11-01', '2023-02-28')})
#dates['date']=dates['date']
#df = pd.DataFrame()
#df['Date'] = dr

cal = calendar()
holidays = cal.holidays(start=dates["datetime"].min(), end=dates["datetime"].max())
#holidays.dt.date
dates["Holiday"] = dates["datetime"].isin(holidays)
dates["Date"]=dates["datetime"].dt.date
dates["Hol_Non_Hol"]=dates['Holiday'].map({True:"Holiday" ,False:"Non-Holiday"})
dates=dates.drop(["Holiday","datetime"],axis=1)
display(dates)

# COMMAND ----------

from pyspark.sql import SparkSession
#Create PySpark SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()
#Create PySpark DataFrame from Pandas
dates=spark.createDataFrame(dates) 
dates.show()

# COMMAND ----------

#.orderBy("member_casual","rideable_type","Hol_Non_Hol","num_rides")
df_hol=df[["rideable_type","member_casual","ride_id","started_at"]]
df_hol=df_hol.withColumn("date",to_date("started_at"))

df_hol_v1=df_hol.join(dates,df_hol.date == dates.Date,"inner").select("rideable_type","member_casual","Hol_Non_Hol","started_at","ride_id",df_hol.date)
df_hol_v1
agg_hol = df_hol_v1.groupBy("rideable_type", "member_casual", "Hol_Non_Hol","date").agg({"ride_id":"count"}).withColumnRenamed("count(ride_id)","num_rides")
display(agg_hol)

# COMMAND ----------

agg_hol_v1=agg_hol.groupBy("rideable_type", "member_casual", "Hol_Non_Hol").agg({"num_rides":"mean"}).withColumnRenamed("average(num_rides)","avg_rides").orderBy("rideable_type","member_casual","Hol_Non_Hol")
display(agg_hol_v1)

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
