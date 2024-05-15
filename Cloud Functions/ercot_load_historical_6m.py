import functions_framework
import gridstatus
import pandas as pd
import datetime
import logging
from google.cloud import storage

# Developer: Shashank
# Purpose: This script defines an HTTP Cloud Function to fetch ERCOT load data for the last 6 months 
#          (from 3rd Nov 2023 to 3rd May 2024) and store it as a CSV file in a designated cloud bucket.

# Initialize logging
logging.basicConfig(level=logging.INFO)

@functions_framework.http
def ercot_load_historical_6m(request):
    """HTTP Cloud Function.
    This function fetches the ERCOT load data from last 6 months (3rd Nov 2023 to 3rd May 2024) 
    and creates a CSV file in the designated cloud bucket.
    """
    try:
        iso = gridstatus.Ercot()

        # Retrieve ERCOT load data for the last 6 months
        df = iso.get_load(start='2023-11-10', end='2024-05-10')

        # Check if DataFrame is empty
        if df.empty:
            logging.warning("No data found for the specified date range.")
            return "No data found for the specified date range."

        timestamp = datetime.datetime.now()
        # Updated filename format with underscores and dashes
        filename = f"ercot_load_6m_{timestamp.strftime('%Y-%m-%d_%H-%M-%S')}.csv"

        bucket_name = "ercot_test"
        folder_name = "ercot_load_csv/load_historical"
        destination_blob_name = f"{folder_name}/{filename}"

        # Upload DataFrame to Cloud Storage
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