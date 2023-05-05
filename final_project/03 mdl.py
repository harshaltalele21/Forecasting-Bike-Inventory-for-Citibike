# Databricks notebook source
# MAGIC %run ./includes/includes

# COMMAND ----------

!pip install fbprophet

# COMMAND ----------

# Load necessary libraries
from pyspark.sql.functions import to_date, hour
from fbprophet import Prophet

# COMMAND ----------

target_data=spark.sql("select * from target_variable")
display(target_data.head(2))

# COMMAND ----------

target_df = target_data.toPandas()

# COMMAND ----------

display(target_df)

# COMMAND ----------

# Combine year, month, and date columns to create a datetime column
target_df['datetime'] = target_df.apply(lambda x: pd.to_datetime(f"{x['dateofmonth_sa']}-{x['monthofyr_sa']}-{x['year_sa']}", format="%d-%m-%Y"), axis=1).dt.date

# Print the updated dataframe
display(target_df)


# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# group by date and calculate average net change

df_grouped = target_df.groupby("datetime").agg({"netchange": "mean"}).reset_index()
df_grouped = df_grouped.rename(columns={"datetime": "ds", "netchange": "y"})
display(df_grouped)

# COMMAND ----------

test = target_df.groupby("netchange").agg({"dateofmonth_sa": "mean"}).reset_index()
display(test)

# COMMAND ----------

# Read in the silver table data
#bike_df = spark.read.format("delta").load("dbfs:/FileStore/tables/G04/bike_trip_data")

#dbfs:/FileStore/tables/G04/bike_trip_data

# Convert the date column to datetime
#bike_df = bike_df.withColumn("date", to_date(bike_df.starttime))


# COMMAND ----------

display(spark.sql('select * from silver_weather_info_dynamic'))

# COMMAND ----------

import pandas as pd

# Execute SQL query in Spark and store result in a dataframe
df_spark = spark.sql('select * from silver_weather_info_dynamic')

# Convert Spark dataframe to Pandas dataframe
weather_df = df_spark.toPandas()

# COMMAND ----------

display(weather_df)

# COMMAND ----------

weather_df['datetime'] = pd.to_datetime(weather_df['dt'], unit='s')
display(weather_df)

# COMMAND ----------

weather_df = weather_df[['datetime', 'temp', 'clouds', 'visibility', 'feels_like','humidity']]
weather_df = weather_df.rename(columns={'datetime': 'weather_datetime',
                                        'temp': 'weather_temp',
                                        'clouds': 'weather_clouds',
                                        'visibility': 'weather_visibility',
                                        'feels_like': 'weather_feels_like',
                                        'humidity': 'weather_humidity'})

# COMMAND ----------

display(weather_df)

# COMMAND ----------



# COMMAND ----------

import pandas as pd
df_spark = spark.sql('select * from silver_weather_historic')
historic_weather = df_spark.toPandas()
display(historic_weather)

# COMMAND ----------

historic_weather['datetime'] = pd.to_datetime(historic_weather['dt'],unit='s').dt.date
display(historic_weather)

# COMMAND ----------

weather_df_historic = historic_weather[['datetime', 'temp', 'clouds', 'visibility', 'feels_like','main' ,'humidity']]
weather_df_historic = weather_df_historic.rename(columns={'datetime': 'ds',
                                        'temp': 'weather_temp',
                                        'clouds': 'weather_clouds',
                                        'visibility': 'weather_visibility',
                                        'feels_like': 'weather_feels_like',
                                        'humidity': 'weather_humidity'})

# COMMAND ----------

display(weather_df_historic)

# COMMAND ----------

weather_df_historic['weather_temp'] = pd.to_numeric(weather_df_historic['weather_temp'], errors='coerce')
weather_df_historic['weather_clouds'] = pd.to_numeric(weather_df_historic['weather_clouds'], errors='coerce')
weather_df_historic['weather_visibility'] = pd.to_numeric(weather_df_historic['weather_visibility'], errors='coerce')
weather_df_historic['weather_feels_like'] = pd.to_numeric(weather_df_historic['weather_feels_like'], errors='coerce')
weather_df_historic['weather_humidity'] = pd.to_numeric(weather_df_historic['weather_humidity'], errors='coerce')

df_grouped_hist = weather_df_historic.groupby("ds").agg({"weather_temp": "mean", 
                                                         "weather_clouds": "mean", 
                                                         "weather_visibility": "mean",
                                                         "weather_feels_like": "mean",
                                                         "weather_humidity": "mean"}).reset_index()


# COMMAND ----------

display(df_grouped_hist)

# COMMAND ----------

merged_df = df_grouped.join(df_grouped_hist.set_index('ds'), on='ds', how='left')


# COMMAND ----------

merged_df.dropna(inplace=True)

# COMMAND ----------

display(merged_df)

# COMMAND ----------

test = target_df.groupby("netchange").agg({"dateofmonth_sa": "mean"}).reset_index()

# COMMAND ----------



# COMMAND ----------

display(spark.sql('show tables'))

# COMMAND ----------

display(spark.sql('select * from silver_station_status_dynamic'))

# COMMAND ----------

import pandas as pd

# Execute SQL query in Spark and store result in a dataframe
df_spark = spark.sql('select * from silver_weather_info_dynamic')

# Convert Spark dataframe to Pandas dataframe
weather_df = df_spark.toPandas()

# COMMAND ----------

import pandas as pd
import datetime as dt

# Convert datetime column to Unix timestamp
weather_df['datetime'] = pd.to_datetime(weather_df['dt'],unit='s').dt.date

# View updated dataframe
display(weather_df.head())

# COMMAND ----------

weather_df = weather_df[['datetime', 'temp', 'clouds', 'visibility', 'feels_like', 'humidity']]
weather_df = weather_df.rename(columns={'datetime': 'weather_datetime',
                                        'temp': 'weather_temp',
                                        'clouds': 'weather_clouds',
                                        'visibility': 'weather_visibility',
                                        'feels_like': 'weather_feels_like',
                                        'humidity': 'weather_humidity'})

# COMMAND ----------

weather_df = weather_df.rename(columns={"datetime": "ds", "netchange": "y"})

# COMMAND ----------

display(weather_df)


# COMMAND ----------

weather_df['weather_temp'] = pd.to_numeric(weather_df['weather_temp'], errors='coerce')
weather_df['weather_clouds'] = pd.to_numeric(weather_df['weather_clouds'], errors='coerce')
weather_df['weather_visibility'] = pd.to_numeric(weather_df['weather_visibility'], errors='coerce')
weather_df['weather_feels_like'] = pd.to_numeric(weather_df['weather_feels_like'], errors='coerce')
weather_df['weather_humidity'] = pd.to_numeric(weather_df['weather_humidity'], errors='coerce')

widisplay() = weather_df.groupby("weather_datetime").agg({"weather_temp": "mean", 
                                                         "weather_clouds": "mean", 
                                                         "weather_visibility": "mean",
                                                         "weather_feels_like": "mean",
                                                         "weather_humidity": "mean"}).reset_index()



#weather_pred = weather_df.groupby("weather_datetime").agg({"weather_temp": "mean"}).reset_index()



# COMMAND ----------

display(df_grouped_pred)

# COMMAND ----------

min(df_grouped_pred['weather_datetime'])

# COMMAND ----------

display(merged_df)

# COMMAND ----------

#display(spark.sql('select * from silver_station_status_dynamic'))

# COMMAND ----------

# Aggregate net bike change by hour
#agg_df = bike_df.groupBy("date", hour("starttime")).agg({"net_bike_change": "sum"})

# Rename columns for Prophet compatibility
#agg_df = agg_df.withColumnRenamed("date", "ds").withColumnRenamed("sum(net_bike_change)", "y")


# COMMAND ----------

## FB PROPHET

# COMMAND ----------

merged_df = merged_df.rename(columns={'datetime': 'ds', 'y': 'y', 'weather_temp': 'temp', 'weather_clouds': 'clouds', 'weather_visibility': 'visibility', 'weather_feels_like': 'feels_like', 'weather_humidity': 'humidity'})


# COMMAND ----------

model = Prophet()
model.add_regressor('temp')
model.add_regressor('clouds')
model.add_regressor('visibility')
model.add_regressor('feels_like')
model.add_regressor('humidity')
model.fit(merged_df)


# COMMAND ----------

future_df = pd.DataFrame({'ds': df_grouped_pred['weather_datetime']})

# COMMAND ----------

future_df['temp'] = df_grouped_pred['weather_temp']
future_df['clouds'] = df_grouped_pred['weather_clouds']
future_df['visibility'] = df_grouped_pred['weather_visibility']
future_df['feels_like'] = df_grouped_pred['weather_feels_like']
future_df['humidity'] = df_grouped_pred['weather_humidity']
forecast = model.predict(future_df)


# COMMAND ----------

display(forecast)

# COMMAND ----------

fig1 = model.plot(forecast)

# COMMAND ----------



# COMMAND ----------

# Initialize Prophet model
model = Prophet()

# COMMAND ----------

model.add_regressor('weather_temp')
model.add_regressor('weather_clouds')
model.add_regressor('weather_visibility')
model.add_regressor('weather_feels_like')
model.add_regressor('weather_humidity')

# COMMAND ----------

# Fit the model to the data
model.fit(merged_df)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# Make predictions for the next 24 hours
future = model.make_future_dataframe(periods=24, freq="H")
forecast = model.predict(future)

# COMMAND ----------

# Show the forecasted values
display(forecast)

# COMMAND ----------

df_grouped.count()

# COMMAND ----------

fig1 = model.plot(forecast)

# COMMAND ----------

# Load necessary libraries
import mlflow.spark

# Set up MLflow tracking and registry
mlflow.set_tracking_uri("<YOUR_TRACKING_URI>")
mlflow.set_registry_uri("<YOUR_REGISTRY_URI>")


# COMMAND ----------

# Start an MLflow experiment run
with mlflow.start_run():
  # Train the model
  model = Prophet().fit(agg_df)
  
  # Log the model with MLflow
  mlflow.spark.log_model(spark_model=model, artifact_path="model")
  
  # Register the model with MLflow registry
  mlflow.register_model("biketrip_forecast", "model")

# COMMAND ----------

import json

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
