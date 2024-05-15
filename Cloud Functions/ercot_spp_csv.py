import functions_framework
import gridstatus
import json
import datetime
from google.cloud import storage
import logging

# Developer: Bingqi
# Purpose: This script defines an HTTP Cloud Function to fetch ERCOT SPP data and store it as a CSV file
#          in a designated cloud bucket.

# Initialize logging
logging.basicConfig(level=logging.INFO)

@functions_framework.http
def ercot_spp_csv(request):
    """HTTP Cloud Function.
    This function fetches the ERCOT SPP data and creates a CSV
    file in the designated cloud bucket.
    """
    try:
        iso = gridstatus.Ercot()

        # Fetch ERCOT SPP data
        df = iso.get_spp(date="latest", market="REAL_TIME_15_MIN", location_type="Load Zone")

        # Convert Timestamp columns to string format
        df['Time'] = df['Time'].astype(str)
        df['Interval Start'] = df['Interval Start'].astype(str)
        df['Interval End'] = df['Interval End'].astype(str)

        csv_data = df.to_dict(orient="records")
        timestamp = datetime.datetime.now()
        filename = f"ercot_spp_{timestamp.strftime('%Y-%m-%d_%H-%M-%S')}.csv"

        bucket_name = "ercot_test"
        folder_name = "ercot_spp_csv/spp_latest"
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