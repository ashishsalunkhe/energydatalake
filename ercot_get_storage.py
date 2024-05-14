import functions_framework
import gridstatus
import pandas as pd
from google.cloud import storage
from io import BytesIO


# def upload_blob(bucket_name, data, destination_blob_name):
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(destination_blob_name)
#     blob.upload_from_string(data)

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
def ercot_get_storage(request):
    """HTTP Cloud Function to get CAISO storage data
     Return storage charging or discharging for today in 5 minute intervals
     Negative means charging, positive means discharging."""
     
    iso = gridstatus.CAISO()  # Using CAISO class

    # Fetch storage data
    data_dict = iso.get_storage(date="latest")  # Assuming 'latest' fetches the most recent data

    # Wrap scalar values in a list and convert dictionary to DataFrame
    for key in data_dict:
      data_dict[key] = [data_dict[key]]
      
    df = pd.DataFrame(data_dict)

    csv_data = df.to_csv(index=False).encode()

    bucket_name = 'ercot_test_bing'
    folder_name = 'ercot_files_folder_bing'
    destination_blob_name = folder_name + "/ercot_storage.csv"

    upload_blob(bucket_name, csv_data, destination_blob_name)

    return "CSV file generated from CAISO storage data and stored in Cloud Storage"
