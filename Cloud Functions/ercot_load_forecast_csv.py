import functions_framework
import gridstatus
import pandas as pd
import datetime
import logging
from google.cloud import storage

# Developer: Shashank
# Purpose: This script defines an HTTP Cloud Function to fetch ERCOT load forecast data
#          and store it as a CSV file in a designated cloud bucket.

# Configure logging
logging.basicConfig(level=logging.INFO)


@functions_framework.http
def ercot_load_forecast_csv(request):
    """HTTP Cloud Function.
    This function fetches the ERCOT load forecast data and creates a CSV
    file in the designated cloud bucket.
    """
    try:
        iso = gridstatus.Ercot()

        # Fetch ERCOT load forecast data
        df = iso.get_load_forecast(date='today')

        timestamp = datetime.datetime.now()
        # Updated filename format with underscores and dashes
        filename = f"ercot_load_forecast_{timestamp.strftime('%Y-%m-%d_%H-%M-%S')}.csv"

        bucket_name = "ercot_test"
        folder_name = "ercot_load_forecast_csv"
        destination_blob_name = f"{folder_name}/{filename}"

        # Upload data to Cloud Storage
        upload_blob(bucket_name, df.to_csv(index=False), destination_blob_name)

        logging.info(f"{filename} stored to Cloud Storage Bucket")
        return f"{filename} stored to Cloud Storage Bucket"
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return "An error occurred"


def upload_blob(bucket_name, data, destination_blob_name):
    """Uploads data to a Google Cloud Storage bucket."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(data)
    except Exception as e:
        logging.error(f"Error uploading blob: {str(e)}")
        raise e  # Re-raise the exception for proper handling by the caller