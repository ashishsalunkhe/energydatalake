import logging
from google.cloud import storage
from pyspark.sql import SparkSession
from google.cloud import bigquery 
# Initialize logging
logging.basicConfig(level=logging.INFO)

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("HistoricalWeatherDataETL") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.0") \
    .getOrCreate()

# Define cloud storage paths
source_folder_path = "gs://openmeteo-weather/hourly-historical-weather-data"
destination_folder_path = "gs://openmeteo-weather/merged-historical-weather-data"
temporary_gcs_bucket = "ercot_test"

# Create a Cloud Storage client
storage_client = storage.Client()

# Function to check if a folder in Cloud Storage contains any files
def check_folder_has_files(bucket_name, folder_path):
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    return any(blobs)

# Check if source folder has files
if not check_folder_has_files("openmeteo-weather", "hourly-historical-weather-data"):
    logging.info("No files found in the source folder. Exiting...")
    # Add any additional handling or return statement if needed
else:
    # Read and merge CSV files into one dataframe
    dfs = []
    blobs = storage_client.list_blobs("openmeteo-weather", prefix="hourly-historical-weather-data/")
    for blob in blobs:
        if blob.name.endswith('.csv'):
            df = spark.read.option("header", "true").csv(f"gs://{blob.bucket.name}/{blob.name}")
            dfs.append(df)

    # Combine all DataFrames into one
    merged_df = dfs[0]
    for df in dfs[1:]:
        merged_df = merged_df.union(df)

    # Data cleaning

    # Assuming data cleaning steps here
    # For demonstration, let's assume we drop null values
    merged_df = merged_df.dropna()

    # Convert "date" column to timestamp format with timezone offset (-05:00)
    merged_df = merged_df.withColumn("date", merged_df["date"].cast("timestamp"))

    # Display unique zones and their count
    zone_counts = merged_df.groupBy("zone").count()
    logging.info("Unique zones and their count:")
    zone_counts.show()

    # Write merged DataFrame to destination folder
    logging.info("Writing merged DataFrame to destination folder...")
    merged_df.write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(destination_folder_path)

    logging.info("Merged DataFrame successfully written to destination folder.")

    # Define BigQuery table schema
    schema = [
        bigquery.SchemaField("latitude", "FLOAT"),
        bigquery.SchemaField("longitude", "FLOAT"),
        bigquery.SchemaField("date", "TIMESTAMP"),
        bigquery.SchemaField("temperature_2m", "FLOAT"),
        bigquery.SchemaField("relative_humidity_2m", "FLOAT"),
        bigquery.SchemaField("dew_point_2m", "FLOAT"),
        bigquery.SchemaField("precipitation", "FLOAT"),
        bigquery.SchemaField("rain", "FLOAT"),
        bigquery.SchemaField("snowfall", "FLOAT"),
        bigquery.SchemaField("cloud_cover", "FLOAT"),
        bigquery.SchemaField("cloud_cover_low", "FLOAT"),
        bigquery.SchemaField("cloud_cover_mid", "FLOAT"),
        bigquery.SchemaField("cloud_cover_high", "FLOAT"),
        bigquery.SchemaField("wind_speed_10m", "FLOAT"),
        bigquery.SchemaField("wind_speed_100m", "FLOAT"),
        bigquery.SchemaField("wind_direction_10m", "FLOAT"),
        bigquery.SchemaField("wind_direction_100m", "FLOAT"),
        bigquery.SchemaField("wind_gusts_10m", "FLOAT"),
        bigquery.SchemaField("zone", "STRING"),  # Add zone field to schema
    ]

    # Define BigQuery table ID
    project_id = "driven-stage-365620"
    dataset_id = "ercot_merged"
    table_id = "historical_weather_data"

    # Write merged DataFrame to BigQuery
    logging.info("Writing merged DataFrame to BigQuery...")
    merged_df.write \
        .format("bigquery") \
        .option("temporaryGcsBucket", temporary_gcs_bucket) \
        .option("table", f"{project_id}.{dataset_id}.{table_id}") \
        .mode("overwrite") \
        .save()

    logging.info("Merged DataFrame successfully written to BigQuery.")

# Stop SparkSession
spark.stop()