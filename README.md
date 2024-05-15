# INST767_Project_Team-6 Ercot and weather Energy Data Lake

## Team Details

| Name              |
|-------------------|
| Ushasri Bhogaraju |
| Aditya Kiran      |
| Shashank Ramprasad|
| Ashish Salunkhe   |
| Bingqi Lian       |

## Summary

Our Data pipeline ingests data from 5 APIs and loads it as is in cloud storage. Our cloud functions fetch the data from relevant APIs. Our workflows execute the relevant Pyspark jobs to pick up all the files in the cloud storage, transform the data to represent correct data types, remove nulls and duplicates, merge data where required, to populate the corresponding BigQuery tables and then move the files to archive folder. All these actions are synchronized using Cloud schedulers.


## Electric Reliability Council of Texas (ERCOT)

The Electric Reliability Council of Texas (ERCOT) manages the flow of electric power to more than 26 million Texas customers -- representing about 90 percent of the state’s electric load. As the independent system operator for the region, ERCOT schedules power on an electric grid that connects more than 54,100 miles of transmission lines and 1,250 generation units, including Private Use Networks. It also performs financial settlement for the competitive wholesale bulk-power market and administers retail switching for 8 million premises in competitive choice areas.

### ercot_fuel_mix:
This API provides insights into the energy sources utilized for electricity generation within the ERCOT region.

### ercot_load_forecast & ercot_load:
These APIs offer data on both forecasted and actual electricity demand across various regions within ERCOT. 

### ercot_spp:
It includes information on energy prices within the ERCOT markets based on load zones.

**Documentation of All Endpoints:** 

* https://www.ercot.com/services/mdt/data-portal
* https://docs.gridstatus.io/en/latest/index.html
* https://www.gridstatus.io/datasets
* https://apiexplorer.ercot.com/

## OpenMeteo Data:

This API provides an extensive range of weather data points that can be correlated with energy consumption patterns:

### Temperature_2m:
Directly impacts heating and cooling requirements, thus influencing energy demand throughout different seasons.

### Precipitation:
Heavy rainfall or snowfall can disrupt energy production from sources like solar or wind, affecting overall energy availability and grid stability.

### Wind Speed:
Wind speed and direction play a significant role in wind power generation, making this data crucial for assessing the impact of weather on renewable energy production.

### Cloud Cover:
Cloud cover directly affects solar energy production by obstructing sunlight. Monitoring cloud cover data helps in understanding the variability in solar energy generation and its impact on the energy grid.

### Example Endpoint

https://archive-api.open-meteo.com/v1/archive?latitude=52.52&longitude=13.41&start_date=2024-04-29&end_date=2024-05-13&hourly=temperature_2m

## OpenWeather Current Weather Data API

This API provides access to current weather data, including minute-by-minute forecast for 1 hour and hourly forecast for 48 hours.

Endpoint: https://api.openweathermap.org/data/2.5/weather


**Parameters:**
- `lat` (required): Latitude of the location. If you need geocoding services, you can use the OpenWeather Geocoding API.
- `lon` (required): Longitude of the location. If you need geocoding services, you can use the OpenWeather Geocoding API.
- `appid` (required): Your unique API key, which can be found on your OpenWeather account page under the "API key" tab.
- `mode` (optional): Response format. Possible values are `xml` and `html`. If not specified, JSON format is used by default.
- `units` (optional): Units of measurement. Possible values are `standard`, `metric`, and `imperial`. If not specified, standard units are used by default.
- `lang` (optional): Language parameter to specify the output language.

**Example Request:**
https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API key}


## ELT Diagram

![ELT Diagram](Images/architecture.png)

## Step 1: Storage Buckets

We created Storage Buckets to hold incoming data. Our data storage bucket is named ercot_test. It is structured to hold incoming CSV files into respective folders. The structure is available in the image below.

![Storage Buckets](Images/team6_bucket_storage_structure.png)

## Step 2: Cloud Functions

We created cloud functions that access data and save to the respective folders.

![Cloud Functions](Images/team6_cloud_functions.png)

### Our Cloud Functions

We have 5 cloud functions to pull in data applying the following 4 methods on the gridstatus iso
iso =  gridstatus.Ercot()
* get_fuel_mix(),
* get_load(), (Source for 2 functions, one for load_latest and one for load_historical)
* get_load_forecast()
* Weather

Additionally, we created two more cloud functions that loads historical data upto the past 6 months:
* Ercot_load_historical_6m: This cloud function fetches the load information from ERCOT for the last 6 months and stores it in a bucket. 
* Store_hourly_hist_weather_6m: Similarly, this cloud function fetches the historical weather data from the last 6 months and stores it in a bucket.

![Cloud Functions](Images/team6_cloud_functions.png)

## Step 3: Spark Jobs

We created Spark jobs that took the csv files, applied transformations such as

* Reading all the existing files in the folder and merging them into a single dataframe
* Transforming column names by replacing spaces with ‘_’ to suit updation to Bigquery
* The schema of the csv files was interpreted as strings by Pyspark. Therefore, we applied relevant transformations to correct the schema
* We corrected float numbers to 2 decimal places
* We deleted Nulls
* We iterated over all the CSV files in the source buckets, copying each one into an archive folder, before removing them from their original locations.
* Basic logging is also implemented to capture information such as the dataframe's shape and the presence of null values. These logging statements are helpful for debugging, monitoring the execution flow and understanding the data being transformed
* For the one pipeline that merges fuel mix and load data, we matched the data on the ‘Time’ column and removed only non-null rows

We saved the spark jobs code to our code storage bucket named store_dataproc_files. We have a code_notebooks folder in it, wherein we have the final_code folder which has Pyspark_code and cloud_functions code subfolders.  Its structure is as follows:

![Spark Jobs](Images/image7.png)

## Step 4: Dataproc Workflows

We then created Dataproc-Workflows, to create 4 distinct pipelines. They are:

* eda-wf-ercot-load-forecast -13: Executes the pyspark job related to reading all the files in the ercot_ load_corecast_csv folder, merging them into a single dataframe, apply transformations, update to bigquery and then move all the files to an archive. 
* ercot-load-hist-13: Executes the pyspark job related to reading all the files in the ercot_ load_csv/fm_historical folder, merging them into a single dataframe, apply transformations, update to bigquery and then move all the files to an archive. 
* ercot-merge-fm-load-13 : Executes the pyspark job related to reading all the files in the ercot_ load_csv/load_latest and ercot_fm_csv/fm_latest  folder, merging them into a single dataframe, apply transformations, update to bigquery and then move all the files to an archive. 
* Eda-wf-ercot-load-latest: Executes the pyspark job related to reading all the files in the ercot_ load_csv/load_latestl folder, merging them into a single dataframe, apply transformations, update to bigquery and then move all the files to an archive. 
* Workflow-spp-weather-merge : Executes the pyspark job which reads all the files in spp_latest folder that contains all the prices of fuels in 4 zones and quarter_hourly_weather_data folder that contains the weather data for the 4 zones. It then cleans and joins the two datasets on the location column(zones) and the time column.
* mergeHistoricalWeather.py : This pyspark job merges all the historical weather data that is fetched from the OpenMeteo API.
* pyspark_ercot_load_latest_BQ_archive_csv : Executes the pyspark job that reads all csv files from load_latest folder, merges them, transforms datatypes, updates big query table, archives the files and deletes from source folder. 

![Dataproc Workflows](Images/image4.png)

## BigQuery Data Pipeline with Cloud Scheduler

This document describes the strategy for ensuring only new files are processed by BigQuery and the Cloud Scheduler configuration for running the pipeline.

### Strategy for New File Processing

Instead of fetching specific files, the process involves:

1. Processing all files present in the designated bucket.
2. Moving processed files to an archive folder upon completion.

### Cloud Schedulers and Tasks

Cloud Schedulers are configured using cron jobs to trigger Cloud Functions and Dataproc cluster jobs at regular intervals.

**List of Schedulers:**

| Scheduler Name                 | Task Performed                                                | Frequency                                           |
|-------------------------------|--------------------------------------------------------------|----------------------------------------------------|
| `eda-wf-ercot-load-forecast`    | Executes ercot load historical workflow                        | Every 12 hours, at 00:00 (daily)                 |
| `eda-wf-ercot-load-hist`         | Executes ercot-load-hist-wf workflow                         | Every 12 hours, at 00:00 (daily)                 |
| `eda-wf-ercot-merge-fm-load`      | Executes ercot merge fuel mix and load latest workflow      | Every 12 hours, at 00:00 (daily)                 |
| `ercot-fm-latest`               | Executes ercot_fm_latest_csv cloud function                  | Every hour, at 15 past the hour (daily)        |
| `ercot-load-historical`        | Executes ercot_load_historical_csv cloud function            | Every hour, at 00:00 (daily)                       |
| `ercot-load-latest`             | Executes ercot_load_latest_csv cloud function                 | Every hour, at 15 past the hour (daily)        |
| `ercot_load_forecast`          | Executes ercot_load_forecast_csv cloud function              | Every hour, at 15 past the hour (daily)        |
| `Quarter_hourly_spp_csv`        | Executes ercot_spp_csv function                             | Every 15 minutes (hourly, daily)                    |
| `Quater_hourly_weather`        | Executes open_weather_live_data function                     | Every 15 minutes (hourly)                             |
| `Sparkjob-spp-weather-merge`     | Executes Spark job to merge SPP and weather data           | Every 12 hours                                       |

![Schedulers](Images/team6_cloud_scheduler_image.png)

### Data Storage in BigQuery

After processing, data is loaded into BigQuery tables:


![Data Storage](Images/image2.png)

* `Ercot_fm_load_merged`
![Data Storage](Images/image5.png)
![Data Storage](Images/image15.png)

* `Ercot_load_forecast`
![Data Storage](Images/image16.png)
![Data Storage](Images/image1.png)

* `Ercot_load_historical`
![Data Storage](Images/image3.png)
![Data Storage](Images/image11.png)

* `Ercot_load_latest`
![Data Storage](Images/image9.png)
![Data Storage](Images/image6.png)

* `Ercot_spp_weather_merged`
![Data Storage](Images/image.png)
![Data Storage](Images/image17.png)



### Data Analysis with Looker

Queries are used to fetch data from BigQuery for business analysis. Visualizations are then created using Google Looker.


![Data Storage](Images/Energy_Generation_and_Load_Consumption_Over_Time.png) 
It appears that generation is highest during the middle of the day, which is likely due to solar power generation. Consumption is highest in the morning and evening, when people are typically awake and using appliances.  Overall, nuclear and natural gas appears to be following a predictable pattern, while solar and wind is more erratic.

![Data Storage](Images/Load-forecast-viz.png)
The chart illustrates the forecasted electrical load spanning three days across five locations: Houston, North, South, West, and the system total. It visually demonstrates a trend where the total energy demand increases throughout the day, peaking during daytime hours and tapering off during the night. This trend aligns with typical human activity patterns.

![Data Storage](Images/weather-and-spp-over-time.png)
The graph depicts the Settlement point priceprice (SPP) alongside weather conditions (Humidity, Temperature, and Wind Speed) over the course of a day
It appears that there is not a clear correlation between the weather conditions and the price of energy. However, it is important to note that this data is only for one day, so it may not be representative of a typical day or season.


### Queries

Business Question 1:

"What is the average energy consumption per month?"

Explanation:

"This query calculates the average energy consumption for each month by extracting the month from the timestamp INTERVAL_START and averaging the load values for each month. This analysis provides insights into the seasonal variation in energy consumption, which can be valuable for resource planning and demand forecasting."

Query:
SELECT 
    EXTRACT(MONTH FROM INTERVAL_START) AS month,
    AVG(load) AS average_load
FROM 
    ercot_merged.ercot_fm_load_merged
GROUP BY 
    month
ORDER BY 
    month;

Business Question 2:

"How does energy consumption vary throughout the day?"

Explanation:

"This query calculates the average energy consumption for each hour of the day by extracting the hour component from the timestamp INTERVAL_START and averaging the load values for each hour. Analyzing energy consumption patterns throughout the day can help identify peak usage hours, which are critical for grid management and capacity planning."

SELECT 
    EXTRACT(HOUR FROM INTERVAL_START) AS hour_of_day,
    AVG(load) AS average_load
FROM 
    ercot_merged.ercot_fm_load_merged
GROUP BY 
    hour_of_day
ORDER BY 
    hour_of_day;


Business Question 3:

"What is the percentage distribution of different energy sources in the overall energy mix?"

Explanation:

"This query calculates the percentage contribution of each energy source to the total energy mix. It divides the sum of each energy source by the sum of all energy sources (coal_and_lignite, hydro, nuclear, power_storage, solar, wind, natural_gas, and other), and then multiplies by 100 to get the percentage. Understanding the distribution of energy sources helps in assessing the reliance on different energy types and planning for a more diversified energy portfolio."

SELECT 
    ROUND(SUM(coal_and_lignite) / SUM(coal_and_lignite + hydro + nuclear + power_storage + solar + wind + natural_gas + other) * 100, 2) AS coal_and_lignite_percent,
    ROUND(SUM(hydro) / SUM(coal_and_lignite + hydro + nuclear + power_storage + solar + wind + natural_gas + other) * 100, 2) AS hydro_percent,
    ROUND(SUM(nuclear) / SUM(coal_and_lignite + hydro + nuclear + power_storage + solar + wind + natural_gas + other) * 100, 2) AS nuclear_percent,
    ROUND(SUM(power_storage) / SUM(coal_and_lignite + hydro + nuclear + power_storage + solar + wind + natural_gas + other) * 100, 2) AS power_storage_percent,
    ROUND(SUM(solar) / SUM(coal_and_lignite + hydro + nuclear + power_storage + solar + wind + natural_gas + other) * 100, 2) AS solar_percent,
    ROUND(SUM(wind) / SUM(coal_and_lignite + hydro + nuclear + power_storage + solar + wind + natural_gas + other) * 100, 2) AS wind_percent,
    ROUND(SUM(natural_gas) / SUM(coal_and_lignite + hydro + nuclear + power_storage + solar + wind + natural_gas + other) * 100, 2) AS natural_gas_percent,
    ROUND(SUM(other) / SUM(coal_and_lignite + hydro + nuclear + power_storage + solar + wind + natural_gas + other) * 100, 2) AS other_percent
FROM 
    `ercot_merged.ercot_fm_load_merged`;




Business Question 4:

"How does weather condition affect electricity prices?"


Explanation:

"This query calculates the average electricity prices (SPP) along with the associated weather conditions such as temperature, humidity, and wind speed. By examining the relationship between weather parameters and electricity prices, we can identify correlations and potential factors influencing price fluctuations."

Query:

SELECT 
    ROUND(AVG(SPP), 2) AS average_price,
    Temperature,
    Humidity,
    Wind_Speed
FROM 
    driven-stage-365620.ercot_merged.ercot_spp_weather_merged
GROUP BY 
    Temperature, Humidity, Wind_Speed
ORDER BY 
    average_price DESC;


## References

* Electric Reliability Council of Texas. (n.d.). About ERCOT. Retrieved February 25, 2024, from https://www.ercot.com/about
* Gridstatus. (n.d.). ERCOT Grid Status. Retrieved February 25, 2024, from https://www.gridstatus.io/live/ercot
* Gridstatus. (n.d.). Gridstatus Documentation. Retrieved February 25, 2024, from https://docs.gridstatus.io/en/latest/index.html
* OpenWeather. (n.d.). OpenWeatherMap API Documentation. Retrieved February 25, 2024, from https://openweathermap.org/api
* Open-Meteo. (n.d.). Open-Meteo API Documentation. Retrieved May 19, 2024, from https://open-meteo.com/en/docs
