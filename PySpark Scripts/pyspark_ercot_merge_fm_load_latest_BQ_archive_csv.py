#This code reads csv files from 2 folders in cloud storage, merges them, transforms datatypes, updates big query table, archives the files and deletes from source folder
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum
from google.cloud import storage
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import FloatType, DecimalType
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)
# Spark session
spark = SparkSession.builder \
    .appName("ercotFuelApp") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.0") \
    .getOrCreate()

# Cloud storage paths
primary_folder_path = "gs://ercot_test/ercot_fm_csv/fm_latest"
secondary_folder_path = "gs://ercot_test/ercot_load_csv/load_latest"
destination_table = "driven-stage-365620.ercot_merged.ercot_fm_load_merged"
temporary_gcs_bucket = "ercot_test"
archive_folder_name = "ercot_test/ercot_archive_csv"


# Create a Cloud Storage client
storage_client = storage.Client()

# Define a function to check if a folder in Cloud Storage contains any files
def check_folder_has_files(bucket_name, folder_path):
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    return any(blobs)

# Check if primary folder has files
if not check_folder_has_files("ercot_test", "ercot_fm_csv/fm_latest"):
    logging.info("No files found in the primary folder. Exiting...")
    # Add any additional handling or return statement if needed
else:
    # Proceed with reading and cleaning dataframes
    df_primary = spark.read.option("header", "true").csv(primary_folder_path)

# Check if secondary folder has files
if not check_folder_has_files("ercot_test", "ercot_load_csv/load_latest"):
    logging.info("No files found in the secondary folder. Exiting...")
    # Add any additional handling or return statement if needed
else:
    # Proceed with reading and cleaning dataframes
    df_secondary = spark.read.option("header", "true").csv(secondary_folder_path)
# Reading and cleaning dataframes

logging.info(f"{df_primary.count()}")
logging.info(f"{df_secondary.count()}")

# #Column name changes
# df_primary_clean = df_primary.select([col(c).alias(c.replace(' ', '_').lower()) for c in df_primary.columns])
# df_secondary_clean = df_secondary.select([col(c).alias(c.replace(' ', '_').lower()) for c in df_secondary.columns])

# Reading and cleaning dataframes using Pandas
df_primary_pd = df_primary.toPandas()
df_secondary_pd = df_secondary.toPandas()

#Column name changes in Pandas DataFrames
df_primary_clean = df_primary_pd.rename(columns=lambda x: x.replace(' ', '_').lower())
df_secondary_clean = df_secondary_pd.rename(columns=lambda x: x.replace(' ', '_').lower())

# Convert 'time' column to datetime
df_primary_clean['time'] = pd.to_datetime(df_primary_clean['time'], utc=True)
df_secondary_clean['time'] = pd.to_datetime(df_secondary_clean['time'], utc=True)

df_primary_clean.sort_values(by='time', inplace=True)
df_secondary_clean.sort_values(by='time', inplace=True)


# Outer join of above dataframes
primary_key = "time"
#df_merged = df_primary_clean.join(df_secondary_clean, primary_key, "outer")
df_merged = pd.merge_asof(df_primary_clean, df_secondary_clean, on = ["time"])
df_merged.dropna(inplace=True)
df_merged = spark.createDataFrame(df_merged)
#Logging-1
logging.info(f"Number of rows in merged dataframe before datatype transformation: {df_merged.count()}")

#Printing merged_df before transformation
df_merged.show(5)

null_counts = df_merged.select([sum(col(c).isNull().cast('int')).alias(c) for c in df_merged.columns])
logging.info(f"Null counts : {null_counts}")

#datatype Transformations and checking for nulls
# Converting 'time' columns to timestamp
df_merged = df_merged.withColumn('time', to_timestamp(col('time')))
df_merged = df_merged.withColumn('interval_start', to_timestamp(col('interval_start')))
df_merged = df_merged.withColumn('interval_end', to_timestamp(col('interval_end')))

# Converting other columns to float and handling null values
float_columns = ['coal_and_lignite', 'hydro', 'nuclear', 'power_storage', 'solar', 'wind', 'natural_gas', 'other', 'load']
decimal_type = DecimalType(10, 2)
for col_name in float_columns:
     df_merged = df_merged.withColumn(col_name, col(col_name).cast(decimal_type))


#Dropping rows with null values
df_merged = df_merged.na.drop()

# Drop duplicate rows
df_merged = df_merged.dropDuplicates()

logging.info(f"Number of rows in merged dataframe after dropping duplicates: {df_merged.count()}")

# Print schema of merged dataframe
print("Schema of merged dataframe:")
df_merged.printSchema()
df_merged.show(5)
# Writing DF to BigQuery
df_merged.write.format('bigquery') \
    .option('table', destination_table) \
    .option('temporaryGcsBucket', temporary_gcs_bucket) \
    .mode('append') \
    .save()
#Reinstate this part of the code after datatype transformation works correctly in big query
# Creating cloud storage client
storage_client = storage.Client()

# Defining source and destination folders for archiving
archive_configurations = [
    {
        "source_bucket_name": "ercot_test",
        "source_folder_name": "ercot_fm_csv/fm_latest",
        "destination_bucket_name": "ercot_test",
        "destination_folder_name": "ercot_archive_csv"
    },
    {
        "source_bucket_name": "ercot_test",
        "source_folder_name": "ercot_load_csv/load_latest",
        "destination_bucket_name": "ercot_test",
        "destination_folder_name": "ercot_archive_csv"
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