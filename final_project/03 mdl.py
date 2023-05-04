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
target_df['datetime'] = target_df.apply(lambda x: pd.to_datetime(f"{x['dateofmonth_sa']}-{x['monthofyr_sa']}-{x['year_sa']}", format="%d-%m-%Y"), axis=1)

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

# Read in the silver table data
#bike_df = spark.read.format("delta").load("dbfs:/FileStore/tables/G04/bike_trip_data")

#dbfs:/FileStore/tables/G04/bike_trip_data

# Convert the date column to datetime
#bike_df = bike_df.withColumn("date", to_date(bike_df.starttime))


# COMMAND ----------

display(spark.sql('select * from silver_weather_info_dynamic'))

# COMMAND ----------

display(spark.sql('select * from silver_station_status_dynamic'))

# COMMAND ----------

# Aggregate net bike change by hour
agg_df = bike_df.groupBy("date", hour("starttime")).agg({"net_bike_change": "sum"})

# Rename columns for Prophet compatibility
agg_df = agg_df.withColumnRenamed("date", "ds").withColumnRenamed("sum(net_bike_change)", "y")


# COMMAND ----------

# Initialize Prophet model
model = Prophet()

# COMMAND ----------

# Fit the model to the data
model.fit(df_grouped)

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
