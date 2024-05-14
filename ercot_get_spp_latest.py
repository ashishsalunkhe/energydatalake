import functions_framework
import gridstatus
import pandas as pd
from google.cloud import storage
import flask

def upload_blob(bucket_name, data, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Check if the blob already exists
    if blob.exists():
        # Download the existing data into a DataFrame
        byte_stream = BytesIO()
        blob.download_to_file(byte_stream)
        byte_stream.seek(0)
        existing_df = pd.read_csv(byte_stream)
        
        # Convert new data string to DataFrame
        new_data_df = pd.read_csv(BytesIO(data))

        # Append new data to existing data
        updated_df = pd.concat([existing_df, new_data_df])

        # Convert updated DataFrame to CSV data
        updated_csv_data = updated_df.to_csv(index=False).encode()
        blob.upload_from_string(updated_csv_data)
    else:
        # If the blob does not exist, simply upload the new data
        blob.upload_from_string(data)

@functions_framework.http
def ercot_get_spp(request):
    """HTTP Cloud Function.
    This function gets the ERCOT real-time market prices by zone and creates a csv 
    file in the designated cloud bucket
    """
    iso = gridstatus.Ercot()

    # print("ISO")

    # Fetch the latest real-time market data by zone
    df = iso.get_spp(date="latest", market="REAL_TIME_15_MIN", location_type="Load Zone")

    # print("DF")

    csv_data = df.to_csv(index=False).encode()

    # print("CSV")

    bucket_name = 'ercot_test_bing'
    folder_name = 'ercot_files_folder_bing'
    destination_blob_name = folder_name + "/ercot_real_time_market.csv"

    upload_blob(bucket_name, csv_data, destination_blob_name)

    return "CSV file generated from ERCOT real-time market prices by zone and stored in Cloud Storage"


