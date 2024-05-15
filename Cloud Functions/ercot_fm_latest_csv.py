import functions_framework
import gridstatus
import pandas as pd
import datetime
from google.cloud import storage
import logging

# Developer: Ushashri
# Purpose: This script defines an HTTP Cloud Function to fetch ERCOT fuel mix data,
#          create a CSV file, and store it in a designated cloud bucket.

# Configure logging
logging.basicConfig(level=logging.INFO)

@functions_framework.http
def ercot_fm_latest_csv(request):
    """HTTP Cloud Function.
    This function fetches the ERCOT fuel mix data and creates a CSV
    file in the designated cloud bucket."""
    try:
        iso = gridstatus.Ercot()

        df = iso.get_fuel_mix(date='latest')

        timestamp = datetime.datetime.now()
        # Updated filename format with underscores and dashes
        filename = f"ercot_fm_{timestamp.strftime('%Y-%m-%d_%H-%M-%S')}.csv"

        bucket_name = "ercot_test"
        folder_name = "ercot_fm_csv/fm_latest"
        destination_blob_name = f"{folder_name}/{filename}"

        upload_blob(bucket_name, df.to_csv(index=False), destination_blob_name)

        return f"{filename} stored to Cloud Storage Bucket"
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return "An error occurred while processing the request."


def upload_blob(bucket_name, data, destination_blob_name):
    """Uploads data to a Google Cloud Storage bucket."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(data)
        logging.info(f"File {destination_blob_name} uploaded successfully to Cloud Storage.")
    except Exception as e:
        logging.error(f"An error occurred while uploading file to Cloud Storage: {str(e)}")
