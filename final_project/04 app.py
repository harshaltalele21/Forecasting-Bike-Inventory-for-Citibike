# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

dbutils.widgets.dropdown("01.start_date", "2023-05-06", ["2023-05-06","2023-05-07", "2023-05-08", "2023-05-09", "2023-05-10", "2023-05-11", "2023-05-12"])
dbutils.widgets.dropdown("02.end_date", "2023-05-06", ["2023-05-06","2023-05-07", "2023-05-08", "2023-05-09", "2023-05-10", "2023-05-11", "2023-05-12"])
dbutils.widgets.dropdown("03.hours_to_forecast", "1", ["1", "2", "3", "4", "5", "6"])
dbutils.widgets.dropdown("04.promote_model", "Yes", ["Yes","No"])

# COMMAND ----------

# DBTITLE 0,YOUR APPLICATIONS CODE HERE...
start_date = str(dbutils.widgets.get('01.start_date'))
end_date = str(dbutils.widgets.get('02.end_date'))
hours_to_forecast = int(dbutils.widgets.get('03.hours_to_forecast'))
promote_model = bool(True if str(dbutils.widgets.get('04.promote_model')).lower() == 'yes' else False)

print(start_date,end_date,hours_to_forecast)

print("YOUR CODE HERE...")

# COMMAND ----------

# To remove any or all widgets
dbutils.widgets.remove("01.start_date")
dbutils.widgets.removeAll()

# COMMAND ----------

display(spark.sql("select * from silver_station_status_dynamic"))

# COMMAND ----------

Forecast the available bikes for the next 4 hours.
Highlight any stock out or full station conditions over the predicted period.
Monitor the performance of your staging and production models using an appropriate residual plot that illustrates the error in your forecasts.  

# COMMAND ----------

# DBTITLE 1,City Bike Station Trip Data & Current Weather
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
 
# Creating a spark session
spark_session = SparkSession.builder.appName(
    'Spark_Session').getOrCreate()
from pyspark.sql.types import StructType,StructField, StringType
schema = StructType([
  StructField('Value', StringType(), True),
  StructField('Category', StringType(), True)])
df=spark.createDataFrame([],schema)

# Current weather info
current_weather=spark.sql("select temp,pop,feels_like,humidity,current_timestamp() as current_time from silver_weather_info_dynamic order by time desc limit 1")

# Current bike station status info
bike_station=spark.sql("select num_ebikes_available,num_docks_available,num_scooters_available,num_bikes_available,num_bikes_disabled,num_docks_disabled from silver_station_status_dynamic order by last_reported_datetime desc limit 1")

rows = [["Current Time",str(current_weather.select("current_time").collect()[0][0])],
["Station Name",GROUP_STATION_ASSIGNMENT],
["Production Model Version","Production Model Version"],
["Staging Model Version","Staging Model Version"],
["Current Temp",str(current_weather.select("temp").collect()[0][0])],
["Current Pop",str(current_weather.select("pop").collect()[0][0])],
["Current Humidity",str(current_weather.select("humidity").collect()[0][0])],
["Total Docks",str(52)],
["Ebikes Available",str(bike_station.select("num_ebikes_available").collect()[0][0])],
["Docks Available",str(bike_station.select("num_docks_available").collect()[0][0])],
["Scooters Available",str(bike_station.select("num_scooters_available").collect()[0][0])],
["Bikes Available",str(bike_station.select("num_bikes_available").collect()[0][0])],
["Bikes Disabled",str(bike_station.select("num_bikes_disabled").collect()[0][0])],
["Docks Disabled",str(bike_station.select("num_docks_disabled").collect()[0][0])]]
columns = ['Category','Value']
 
# Creating the DataFrame
second_df = spark_session.createDataFrame(rows, columns)

first_df = df.union(second_df)
display(first_df)

# COMMAND ----------

# DBTITLE 1,Interactive Map to show station location and name
import folium
# Plot Gaussian means (cluster centers):
center_map = folium.Map(location=[40.74901271, -73.98848395], zoom_start=13,title="Tanvi")
iframe = folium.IFrame(GROUP_STATION_ASSIGNMENT, width=150, height=25)
folium.Marker(location =[40.74901271, -73.98848395],fill_color='#43d9de').add_child(folium.Popup(iframe)).add_to(center_map)

html_string = center_map._repr_html_()

# Display the map 
displayHTML(html_string)


# COMMAND ----------

# DBTITLE 1,Read weather dynamic table to get the forecasted weather data
forecasted_weather=spark.sql("select temp,time from silver_weather_info_dynamic where monthofyr=4 and dateofmonth between 28 and 29 ")
display(forecasted_weather.count())


# COMMAND ----------

# DBTITLE 1,Forecast no of bikes for the next 4 hours 


# COMMAND ----------

# DBTITLE 1,Insert forecasted data to gold table


# COMMAND ----------

# DBTITLE 1,Use actual data from bike station info silver tables and create residuals


# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
