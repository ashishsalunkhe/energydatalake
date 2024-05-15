import functions_framework
import openmeteo_requests
import pandas as pd
from google.cloud import storage
from retry_requests import retry
import requests_cache
import logging
import datetime

# Developer: Ashish
# Purpose: This script defines an HTTP Cloud Function to fetch historical hourly weather data for cities,
#          store it as CSV files in a designated cloud bucket, and upload them to Google Cloud Storage.

# Configure logging
logging.basicConfig(level=logging.INFO)

# Constants
START_DATE = "2023-11-10"
END_DATE = "2024-05-10"
BUCKET_NAME = "openmeteo-weather"
HISTORICAL_DATA_FOLDER = "hourly-historical-weather-data"

# Zone data with names, latitudes, and longitudes
zones_data = {
            "LZ_HOUSTON": {"latitude": 29.763, "longitude": -95.363},
            "LZ_WEST": {"latitude": 32.452, "longitude": -99.718},
            "LZ_SOUTH": {"latitude": 27.801, "longitude": -97.396},
            "LZ_NORTH": {"latitude": 33.578, "longitude": -101.855},
        }


# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)


def validate_zone_data(zone_data):
    """Validate zone data to ensure it contains latitude and longitude.

    Args:
        zone_data (dict): The data for a zone including latitude and longitude.

    Returns:
        bool: True if the zone data is valid, False otherwise.
    """
    return all(key in zone_data for key in ["latitude", "longitude"])


def store_hourly_data_for_zone(zone, zone_data):
    """Store hourly historical weather data for a given zone.

    Args:
        zone (str): The name of the zone.
        zone_data (dict): The data for the zone including latitude and longitude.

    Returns:
        str: The location of the stored data in Cloud Storage, or None if an error occurred.
    """
    try:
        # Make sure all required weather variables are listed here
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": zone_data["latitude"],
            "longitude": zone_data["longitude"],
            "start_date": START_DATE,
            "end_date": END_DATE,
            "hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "precipitation",
                       "rain", "snowfall", "cloud_cover", "cloud_cover_low", "cloud_cover_mid",
                       "cloud_cover_high", "wind_speed_10m", "wind_speed_100m", "wind_direction_10m",
                       "wind_direction_100m", "wind_gusts_10m"]
        }
        responses = openmeteo.weather_api(url, params=params)

        # Process first location. Add a for-loop for multiple locations or weather models
        response = responses[0]
        hourly = response.Hourly()

        # Create a dictionary to store hourly data
        hourly_data = {
            "zone": zone,
            "latitude": zone_data["latitude"],
            "longitude": zone_data["longitude"],
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            )
        }

        # Extract data for each variable
        for i, variable_name in enumerate(params["hourly"]):
            values = hourly.Variables(i).ValuesAsNumpy()
            hourly_data[variable_name] = values

        # Create DataFrame from the hourly data dictionary
        hourly_dataframe = pd.DataFrame(data=hourly_data)

        # Convert DataFrame to CSV format
        csv_data = hourly_dataframe.to_csv(index=False).encode()

        # Define blob name
        blob_name = f"{HISTORICAL_DATA_FOLDER}/{zone}_hourly_weather_data.csv"

        # Upload CSV data to Google Cloud Storage
        upload_blob(BUCKET_NAME, csv_data, blob_name)

        location = f"gs://{BUCKET_NAME}/{blob_name}"
        logging.info(f"Historical hourly weather data for {zone} stored in Cloud Storage at {location}")
        return location
    except Exception as e:
        logging.error(f"Error processing data for {zone}: {str(e)}")
        return None


def upload_blob(bucket_name, data, blob_name):
    """Uploads data to a Google Cloud Storage bucket.

    Args:
        bucket_name (str): The name of the Cloud Storage bucket.
        data (bytes): The data to upload.
        blob_name (str): The name of the blob in the bucket.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data)


@functions_framework.http
def store_hourly_hist_weather(request):
    """HTTP Cloud Function to store historical hourly weather data for cities.

    This function retrieves historical hourly weather data for cities
    and stores it as a CSV file in Google Cloud Storage.

    Returns:
        str: A message indicating the success or failure of the operation.
    """
    try:
        locations = []
        for zone, zone_data in zones_data.items():
            if validate_zone_data(zone_data):
                location = store_hourly_data_for_zone(zone, zone_data)
                if location:
                    locations.append(location)
            else:
                logging.error(f"Invalid data for zone: {zone}")
        
        if locations:
            return f"Historical hourly weather data stored in Cloud Storage. Locations: {', '.join(locations)}"
        else:
            return "No data stored in Cloud Storage. Check logs for details."
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return "An error occurred"


if __name__ == '__main__':
    store_hourly_hist_weather(None)