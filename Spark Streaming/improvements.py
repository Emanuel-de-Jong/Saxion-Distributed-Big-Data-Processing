from pyspark.sql import SparkSession
from pyspark.sql.functions import window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql import functions as F
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import os
import shutil
import threading
import time

matplotlib.use('TkAgg')

# Create Spark Session
SPARK_MASTER = "local[*]"
SPARK_APP_NAME = "dataframes"

spark_session = SparkSession.builder.master(SPARK_MASTER).appName(SPARK_APP_NAME).getOrCreate()

# Define Constants and Schema
DATA_PATH = "data/*.csv"
STREAMING_INPUT_PATH = f"stream-data{os.sep}"

SCHEMA = StructType([
    StructField("vendor_id", StringType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("payment_type", StringType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True)
])

# Load CSV Data

df = spark_session.read.csv(DATA_PATH, schema=SCHEMA)
df.show()

# Helper Functions

def group_and_aggregate(df, window_duration, group_by_column, agg_column, agg_func, alias_name):
    return df.groupBy(
        window(df.pickup_datetime, window_duration),
        group_by_column
    ).agg(
        agg_func(agg_column).alias(alias_name)
    ).orderBy("window", group_by_column)

def convert_to_pandas_and_plot(df, limit=50):
    pd_df = df.limit(limit).toPandas()
    return pd_df

def plot_bar_chart(data, values, xlabel, ylabel, title, rotation=90, figsize=(25, 6)):
    plt.figure(figsize=figsize)
    plt.bar(data, values, width=0.9)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(rotation=rotation)
    plt.grid(axis="y")
    plt.title(title)
    plt.show()

def write_stream_to_memory(df, query_name, output_mode="complete"):
    return df.writeStream.format("memory").queryName(query_name).outputMode(output_mode).trigger(processingTime='10 seconds').start()

def format_window_column(window_column):
    return [w.start.strftime('%y-%m-%d %H') for w in window_column]

# Function to Batch Files for Streaming

def batch_loop():
    csv_paths = []
    data_path = "data"
    stream_path = "stream-data"

    # Clear existing files in stream directory
    for file_name in os.listdir(stream_path):
        if file_name.endswith(".csv"):
            os.remove(os.path.join(stream_path, file_name))

    # Collect all CSV files from the data directory
    for file_name in os.listdir(data_path):
        if file_name.endswith(".csv"):
            csv_paths.append(os.path.join(data_path, file_name))

    batch_size = 5
    seconds_between_batches = 1
    while len(csv_paths) != 0:
        csv_paths_batch = csv_paths[:batch_size]
        csv_paths = csv_paths[batch_size:]

        for path in csv_paths_batch:
            shutil.copy(path, stream_path)
            print(f"Copied {path} to stream-data")

        # Allow time for the streaming job to process the newly added files
        time.sleep(seconds_between_batches)

# Start the batch loop in a separate thread to allow streaming concurrently
batch_thread = threading.Thread(target=batch_loop)
batch_thread.start()

# Streaming Queries
streaming_input_df = (
  spark_session
    .readStream
    .schema(SCHEMA)
    .option("maxFilesPerTrigger", 1)
    .csv(STREAMING_INPUT_PATH)
)

# Streaming Query 1: Average Fare per Hour
streaming_counts_df = group_and_aggregate(
    streaming_input_df, "1 hour", streaming_input_df.fare_amount, "fare_amount", F.mean, "average_fare"
)
query1 = write_stream_to_memory(streaming_counts_df, "average_fare")

# Streaming Query 2: Count of Short Rides (< 2 Miles) per Hour
streaming_short_rides_df = streaming_input_df.filter(streaming_input_df.trip_distance < 2)
streaming_counts_df = streaming_short_rides_df.groupBy(
    window(streaming_short_rides_df.pickup_datetime, "1 hour")
).count().orderBy("window")
query2 = write_stream_to_memory(streaming_counts_df, "short_ride_counts")

# Streaming Query 3: Sliding Window Overview of Number of Taxi Rides per 2 Hours (Updated Every Hour)
sliding_window_df = streaming_input_df.groupBy(
    window(streaming_input_df.pickup_datetime, "2 hours", "1 hour")
).count().orderBy("window")
query3 = write_stream_to_memory(sliding_window_df, "sliding_window_rides")

# Streaming Query 4: Custom Streaming Query: Average Tip per 5 Minutes (Tumbling Window)
tumbling_window_df = streaming_input_df.groupBy(
    window(streaming_input_df.pickup_datetime, "5 minutes")
).agg(
    F.mean("tip_amount").alias("avg_tip")
).orderBy("window")
query4 = write_stream_to_memory(tumbling_window_df, "avg_tip_5min")

# Streaming Query 5: Custom Streaming Query: Count of Rides per 10 Minutes (Sliding Window)
sliding_window_10min_df = streaming_input_df.groupBy(
    window(streaming_input_df.pickup_datetime, "10 minutes", "5 minutes")
).count().orderBy("window")
query5 = write_stream_to_memory(sliding_window_10min_df, "count_rides_10min")

# Wait for the batch loop to complete
batch_thread.join()

# Wait additional time to ensure all files are processed
time.sleep(30)

# Fetch and plot results for all streaming queries
# Average Fare per Hour
average_fare = spark_session.sql("SELECT * FROM average_fare")
pd_df = convert_to_pandas_and_plot(average_fare)
data = format_window_column(pd_df['window'])
plot_bar_chart(data, pd_df['average_fare'], 'Time', 'Average Fare', 'Per hour/day the average fare amount of the rides')

# Count of Short Rides (< 2 Miles) per Hour
counts_df = spark_session.sql("SELECT * FROM short_ride_counts")
pd_df = convert_to_pandas_and_plot(counts_df, limit=100)
data = format_window_column(pd_df['window'])
plot_bar_chart(data, pd_df['count'], 'Time', 'Count of Rides < 2 Miles', 'Per hour the number of rides that were less than 2 miles long')

# Sliding Window Overview of Number of Taxi Rides per 2 Hours (Updated Every Hour)
sliding_window_rides = spark_session.sql("SELECT * FROM sliding_window_rides")
pd_df = convert_to_pandas_and_plot(sliding_window_rides)
data = format_window_column(pd_df['window'])
plot_bar_chart(data, pd_df['count'], 'Time', 'Count', 'Per 2 hours an overview of how many rides had 1 passenger, 2 passengers, 3 passengers, which will be updated every hour. So for every hour you see the number of taxi rides of the last 2 hours')

# Average Tip per 5 Minutes (Tumbling Window)
avg_tip_5min = spark_session.sql("SELECT * FROM avg_tip_5min")
pd_df = convert_to_pandas_and_plot(avg_tip_5min)
data = format_window_column(pd_df['window'])
plot_bar_chart(data, pd_df['avg_tip'], 'Time', 'Average Tip', 'Formulate some queries, you are interested in yourself, using both tumbling and sliding windows. Because of the number of drives in 2.5 weeks (5 million) it might be interesting to look at 5 /10 minutes windows to look at the data')

# Count of Rides per 10 Minutes (Sliding Window)
count_rides_10min = spark_session.sql("SELECT * FROM count_rides_10min")
pd_df = convert_to_pandas_and_plot(count_rides_10min)
data = format_window_column(pd_df['window'])
plot_bar_chart(data, pd_df['count'], 'Time', 'Count', 'Custom Streaming Query: Count of Rides per 10 Minutes (Sliding Window)')

# Stop Spark Session
spark_session.stop()
