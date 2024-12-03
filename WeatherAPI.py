import time
import requests
import json
import sqlite3
import os


# NOAA API details
API_KEY = "WsrAbyxadIzZgtFanYBiXgzRcjaswpuH"  # Replace with your actual API key.
BASE_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
CACHE_FILE = "weather_cache.json"  # File to store cached data


def fetch_with_retry(url, headers, params, retries=5, delay=5):
    """
    Tries to fetch the data from the API with retries on failure (503 errors).
    """
    for attempt in range(retries):
        response = requests.get(url, headers=headers, params=params)
       
        if response.status_code == 503:
            print(f"Attempt {attempt + 1}/{retries} failed with 503. Retrying in {delay} seconds...")
            time.sleep(delay)
        elif response.status_code == 200:
            return response.json()  # Return the response if successful
        else:
            print(f"Error: {response.status_code}, {response.text}")
            return None

 
    print("Max retries reached. Please try again later.")
    return None




def fetch_and_cache_weather_data(start_date, end_date, location_id="CITY:US170006", limit=25):
    """
    Fetch weather data for a specific date range directly from the API and save it to the cache.
    """
    offset = 0  # Start at the first record
    total_records_fetched = 0


    while True:
        # Define API request parameters
        params = {
            "datasetid": "GHCND",
            "locationid": location_id,
            "datatypeid": "TAVG",
            "startdate": start_date,
            "enddate": end_date,
            "units": "metric",
            "limit": limit,
            "offset": offset
        }


        # Set headers with API key
        headers = {"token": API_KEY}


        # Use the retry function to fetch the data
        data = fetch_with_retry(BASE_URL, headers, params)
       
        if data:
            # Extract results
            results = data.get("results", [])
            if not results:
                print(f"No more data available for {start_date} to {end_date}.")
                break


            # Add the fetched data to the cache
            write_to_cache_file(results)


            print(f"Fetched and cached {len(results)} records for {start_date} to {end_date}.")
            total_records_fetched += len(results)


            # Increment offset for the next batch
            offset += limit
        else:
            print(f"Failed to fetch data for {start_date} to {end_date}.")
            break


    print(f"Total records fetched and cached for {start_date} to {end_date}: {total_records_fetched}")



def write_to_cache_file(new_data, cache_file=CACHE_FILE):
    """
    Write data to the cache file without overwriting existing content.
    Ensures only one data point per date is saved in the cache.
    """
    try:
        # Step 1: Read existing content if the file exists
        with open(cache_file, "r") as file:
            existing_data = [json.loads(line) for line in file]
    except FileNotFoundError:
        existing_data = []

    # Step 2: Create a dictionary to keep only one record per date
    data_by_date = {record["date"]: record for record in existing_data}
    
    # Step 3: Update the dictionary with the new data (overwrites duplicates)
    for record in new_data:
        data_by_date[record["date"]] = record

    # Step 4: Write the updated content back to the file
    with open(cache_file, "w") as file:
        for record in data_by_date.values():
            file.write(json.dumps(record) + "\n")

    print(f"Cache updated with {len(new_data)} new records.")


def fetch_weather_for_years(years, days=10, location_id="CITY:US170006", limit=25):
    """
    Fetch weather data for the first 'days' days of every month for the specified years.
    """
    for year in years:
        for month in range(1, 13):  # Loop through months from 1 to 12
            # Format month to ensure two-digit format (e.g., "01", "02", ..., "12")
            month_str = f"{month:02d}"
            
            # Define start and end dates for the first 'days' days of the month
            start_date = f"{year}-{month_str}-01"
            end_date = f"{year}-{month_str}-{days:02d}"
            
            print(f"Fetching weather data for {start_date} to {end_date}...")
            fetch_and_cache_weather_data(start_date, end_date, location_id, limit)

# Specify the years and call the function
fetch_weather_for_years([2000, 2020], days=10, location_id="CITY:US170006", limit=25)

