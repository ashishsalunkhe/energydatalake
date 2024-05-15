#This code reads csv files from 2 folders in cloud storage, merges them, transforms datatypes, updates big query table, archives the files and deletes from source folder
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from google.cloud import storage
from pyspark.sql.functions import col, to_timestamp
import pandas as pd
from pyspark.sql.functions import col, unix_timestamp, expr
import logging

logging.basicConfig(level=logging.INFO)

spark = SparkSession.builder \
    .appName("spp_weather") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.0") \
    .getOrCreate()

primary_folder_path = "gs://ercot_test/ercot_spp_csv/spp_latest"
secondary_folder_path = "gs://openweather_live_data/quarter_hourly_weather_data"
destination_table = "driven-stage-365620.ercot_merged.ercot_spp_weather_merged"
temporary_gcs_bucket = "ercot_test"

# Create a Cloud Storage client
storage_client = storage.Client()

# Define a function to check if a folder in Cloud Storage contains any files
def check_folder_has_files(bucket_name, folder_path):
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    return any(blobs)

# Check if primary folder has files
if not check_folder_has_files("ercot_test", "ercot_spp_csv/spp_latest"):
    logging.info("No files found in the primary folder. Exiting...")
    # Add any additional handling or return statement if needed
else:
    # Proceed with reading and cleaning dataframes
    prices_df = spark.read.option("header", "true").csv(primary_folder_path)
# Check if secondary folder has files
if not check_folder_has_files("openweather_live_data", "quarter_hourly_weather_data"):
    logging.info("No files found in the secondary folder. Exiting...")
    # Add any additional handling or return statement if needed
else:
    # Proceed with reading and cleaning dataframes
    weather_df = spark.read.option("header", "true").csv(secondary_folder_path)

# Reading and cleaning dataframes

# Convert Date and Time into timestamp format considering the timezone offset included in your data (-05:00)
weather_df = weather_df.withColumn("Timestamp", to_timestamp(col("Date"), "yyyy-MM-dd HH:mm:ssXXX"))
prices_df = prices_df.withColumn("Interval Start", to_timestamp(col("Interval Start"), "yyyy-MM-dd HH:mm:ssXXX"))
prices_df = prices_df.withColumn("Interval End", to_timestamp(col("Interval End"), "yyyy-MM-dd HH:mm:ssXXX"))


joined_df = weather_df.alias("weather").join(
    prices_df.alias("prices"),
    (col("weather.Location") == col("prices.Location")) &  # Spatial alignment
    (col("weather.Timestamp").between(col("prices.Interval Start"), col("prices.Interval End"))),  # Temporal alignment
    "inner"
)


final_df = joined_df.select(
    col("weather.Location"),
    col("weather.Temperature").cast("float").alias("Temperature"),
    col("weather.Temp_min").cast("float").alias("Temp_min"),
    col("weather.Temp_max").cast("float").alias("Temp_max"),
    col("weather.Pressure").cast("float").alias("Pressure"),
    col("weather.Humidity").cast("float").alias("Humidity"),
    col("weather.Wind Speed").cast("float").alias("Wind_Speed"),  # Note: Adjusted column name
    col("weather.Timestamp").alias("Weather_Timestamp"),
    col("prices.SPP").cast("float").alias("SPP"),
    to_timestamp(col("prices.Time")).alias("Price_Time"),  # Convert to datetime
    col("prices.Interval Start").alias("Price_Interval_Start"),
    col("prices.Interval End").alias("Price_Interval_End")
)


final_df = final_df.dropDuplicates()
print("Weather DataFrame:")
weather_df.select("Location", "Timestamp").show(truncate=False)

print("Prices DataFrame:")
prices_df.select("Location", "Interval Start", "Interval End").show(truncate=False)

# Show the results to verify the join
final_df.show(truncate=False)

# Write the final DataFrame to BigQuery
final_df.write.format("bigquery") \
    .option("temporaryGcsBucket", temporary_gcs_bucket) \
    .option("table", destination_table) \
    .mode("append") \
    .save()

storage_client = storage.Client()

# Defining source and destination folders for archiving
archive_configurations = [
    {
        "source_bucket_name": "ercot_test",
        "source_folder_name": "ercot_spp_csv/spp_latest",
        "destination_bucket_name": "ercot_test",
        "destination_folder_name": "ercot_archive_csv"
    },
    {
        "source_bucket_name": "openweather_live_data",
        "source_folder_name": "quarter_hourly_weather_data",
        "destination_bucket_name": "openweather_live_data",
        "destination_folder_name": "openweather_archive"
    }
]

# Looping through archive config
for config in archive_configurations:
    
    source_bucket = storage_client.bucket(config["source_bucket_name"])
    destination_bucket = storage_client.bucket(config["destination_bucket_name"])

    # Getting all the present files in the source folder
    blobs = source_bucket.list_blobs(prefix=config["source_folder_name"])

    # Iterating and archiving
    for blob in blobs:
        # Creating destination blob
        if not blob.name.endswith('/'):
            destination_blob_name = blob.name.replace(config["source_folder_name"], config["destination_folder_name"], 1)

            # Copying files to destination blob
            source_blob = source_bucket.blob(blob.name)
            destination_blob = destination_bucket.blob(destination_blob_name)
            destination_blob.upload_from_string(source_blob.download_as_string())

            # Deleting copied file at source folder
            source_blob.delete()
   


#end
spark.stop()