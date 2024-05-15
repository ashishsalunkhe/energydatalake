#This code reads csv files from 1 folder in cloud storage, merges them, transforms datatypes, updates big query table, archives the files and deletes from source folder
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum
from google.cloud import storage
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import FloatType, DecimalType
import logging

logging.basicConfig(level=logging.INFO)
# Spark session
spark = SparkSession.builder \
    .appName("ercotLoadLatestApp") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.22.0") \
    .getOrCreate()

# Cloud storage paths
folder_path = "gs://ercot_test/ercot_load_csv/load_latest"

destination_table = "driven-stage-365620.ercot_merged.ercot_load_latest"
temporary_gcs_bucket = "ercot_test"
archive_folder_name = "ercot_test/ercot_archive_csv"

# Create a Cloud Storage client
storage_client = storage.Client()

# Define a function to check if a folder in Cloud Storage contains any files
def check_folder_has_files(bucket_name, folder_path):
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    return any(blobs)

if not check_folder_has_files("ercot_test", "ercot_load_csv/load_latest"):
    logging.info("No files found in the primary folder. Exiting...")
    # Add any additional handling or return statement if needed
else:
    # Proceed with reading and cleaning dataframes
    df_load_latest_clean = spark.read.option("header", "true").csv(folder_path)


#Column name changes
df_load_latest_clean = df_load_latest_clean.select([col(c).alias(c.replace(' ', '_').lower()) for c in df_load_latest_clean.columns])

#Logging-1
logging.info(f"Number of rows in the dataframe before datatype transformation: {df_load_latest_clean.count()}")

#Printing the combined df before transformation
print("Schema of the dataframe:")
df_load_latest_clean.printSchema()

df_load_latest_clean.show(5)

null_counts = df_load_latest_clean.select([sum(col(c).isNull().cast('int')).alias(c) for c in df_load_latest_clean.columns])
#Logging-2
logging.info(f"Null counts : {null_counts}")

#datatype Transformations and checking for nulls
# Converting 'time' columns to timestamp
df_load_latest_clean = df_load_latest_clean.withColumn('time', to_timestamp(col('time')))
df_load_latest_clean = df_load_latest_clean.withColumn('interval_start', to_timestamp(col('interval_start')))
df_load_latest_clean = df_load_latest_clean.withColumn('interval_end', to_timestamp(col('interval_end')))

# Converting other columns to float and handling null values
float_columns = ['load']
decimal_type = DecimalType(10, 2)
for col_name in float_columns:
     df_load_latest_clean = df_load_latest_clean.withColumn(col_name, col(col_name).cast(decimal_type))

#Dropping Null values
df_load_latest_clean = df_load_latest_clean.na.drop()

# Print schema of transformed dataframe
print("Schema of the cleaned dataframe:")
df_load_latest_clean.printSchema()
df_load_latest_clean.show(5)
# Writing DF to BigQuery
df_load_latest_clean.write.format('bigquery') \
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