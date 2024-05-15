import functions_framework
import os
import requests
import pandas as pd
import datetime
from google.cloud import storage
import pytz
import logging

# Developer: Aditya
# Purpose: This script defines an HTTP Cloud Function to fetch weather data from the OpenWeather API,
#          format it into a DataFrame, and store it as a CSV file in a designated cloud bucket.

# Constants
API_KEY = "8c6d96932235d74117595f9e8423547b"
cities_data = {
    "LZ_HOUSTON": {"latitude": 29.763, "longitude": -95.363},
    "LZ_WEST": {"latitude": 32.452, "longitude": -99.718},
    "LZ_SOUTH": {"latitude": 27.801, "longitude": -97.396},
    "LZ_NORTH": {"latitude": 33.578, "longitude": -101.855},
}

# Configure logging
logging.basicConfig(level=logging.INFO)

@functions_framework.http
def fetch_and_store_weather_data(request):
    """HTTP Cloud Function.
    This function fetches weather data from the OpenWeather API
    and stores it in a CSV file in a cloud bucket."""
    try:
        weather_data = fetch_weather_data()
        df = pd.DataFrame(weather_data)

        timestamp = datetime.datetime.now()
        filename = f"weather_live_{timestamp.strftime('%Y-%m-%d_%H-%M-%S')}.csv"

        bucket_name = "openweather_live_data"
        folder_name = "quarter_hourly_weather_data"
        destination_blob_name = f"{folder_name}/{filename}"

        upload_blob(bucket_name, df.to_csv(index=False), destination_blob_name)

        return f"Weather data uploaded as {filename}"
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return "An error occurred while processing the request."


def fetch_weather_data():
    """Fetch weather data for predefined cities."""
    weather_records = []
    for zone, coords in cities_data.items():
        try:
            url = f"https://api.openweathermap.org/data/2.5/weather?lat={coords['latitude']}&lon={coords['longitude']}&units=imperial&appid={API_KEY}"
            response = requests.get(url)
            weather_data = response.json()
            weather_records.append({
                "Location": zone,
                "Temperature": weather_data["main"]["temp"],
                "Temp_min": weather_data["main"]["temp_min"],
                "Temp_max": weather_data["main"]["temp_max"],
                "Pressure": weather_data["main"]["pressure"],
                "Humidity": weather_data["main"]["humidity"],
                "Wind Speed": weather_data["wind"]["speed"],
                "Date": datetime.datetime.utcfromtimestamp(weather_data['dt']).replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/Chicago'))
            })
        except Exception as e:
            logging.error(f"Error fetching weather data for {zone}: {str(e)}")
    return weather_records


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