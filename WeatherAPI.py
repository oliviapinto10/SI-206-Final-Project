# # API_KEY = "708e7032d77774eb38fa7db6c7635ce1"
# # import requests
# # from datetime import date


# # def generate_dates(years):
# #     dates_list = []
# #     for year in years:
# #         for month in range(1, 13):  # Iterate over each month
# #             for day in range(1, 11):  # Only include days 1 through 10
# #                 dates_list.append(date(year, month, day))
# #     return dates_list
# # years = [2000, 2020]
# # dates = generate_dates(years)
# # # Print or use the list
# # # for d in dates:
# # #     print(d)

# # def get_weather_data(dates):
# #     url = "https://api.weatherstack.com/current?access_key={API-KEY}"

# #     querystring = {"query":"Chicago", "historical_date": dates[:26]}

# #     response = requests.get(url, params=querystring)

# #     print(response.json())



# # Function to generate the first 10 days of every month
# # def generate_dates(years):
# #     dates_list = []
# #     for year in years:
# #         for month in range(1, 13):  # Iterate over each month
# #             for day in range(1, 11):  # Only include days 1 through 10
# #                 dates_list.append(date(year, month, day).strftime("%Y-%m-%d"))
# #     return dates_list

# # # Function to get weather data for a batch of dates with caching
# # def get_weather_data_with_cache(dates, cache_file="weather_cache.json"):
#     # try:
#     #     # Load existing cache
#     #     with open(cache_file, "r") as f:
#     #         cache = json.load(f)
#     # except FileNotFoundError:
#     #     cache = {}

# #     api_key = "708e7032d77774eb38fa7db6c7635ce1"  # Replace with your actual API key
# #     url = f"https://api.weatherstack.com/historical"

# #     for date_str in dates:
# #         if date_str not in cache:  # Skip dates already in cache
# #             querystring = {
# #                 "access_key": api_key,
# #                 "query": "Chicago",
# #                 "historical_date": date_str,
# #                 "hourly": 1
# #             }
# #             response = requests.get(url, params=querystring)
# #             if response.status_code == 200:
# #                 data = response.json()
# #                 if "historical" in data and date_str in data["historical"]:
# #                     cache[date_str] = data["historical"][date_str]
# #                 else:
# #                     print(f"No data for {date_str}")
# #             else:
# #                 print(f"Failed to fetch data for {date_str}: {response.status_code}")
# #                 break  # Stop further API calls if an error occurs

# #     # Save cache back to file
# #     with open(cache_file, "w") as f:
# #         json.dump(cache, f)

# #     return cache

# # # Function to calculate average temperature from hourly data
# # def calculate_average_temperature(data):
# #     if data and "hourly" in data:
# #         temps = [hour["temperature"] for hour in data["hourly"]]
# #         return sum(temps) / len(temps)
# #     return None

# # # Main script
# # years = [2000, 2020]
# # dates = generate_dates(years)

# # # Get weather data with caching
# # weather_cache = get_weather_data_with_cache(dates)

# # # Calculate and print average temperatures
# # for date_str, weather_data in weather_cache.items():
# #     avg_temp = calculate_average_temperature(weather_data)
# #     if avg_temp is not None:
# #         print(f"Date: {date_str}, Average Temperature: {avg_temp}°C")





# # # http://api.weatherstack.com/historicalimport requests
# # import json
# # from datetime import date
# # #     ? access_key = 708e7032d77774eb38fa7db6c7635ce1
# # #     & query = New York
# # #     & historical_date = 2015-01-21
# # #     & hourly = 1
# import requests  # Import the `requests` library to handle HTTP requests.
# import json

# def get_weather_data(cache_file): 
#     try:
#         # Load existing cache
#         with open(cache_file, "r") as f:
#             cache = json.load(f)
#     except FileNotFoundError:
#         cache = {}
# # NOAA API details
#     API_KEY = "WsrAbyxadIzZgtFanYBiXgzRcjaswpuH"  # Replace this with the API key obtained from NOAA's registration.
#     BASE_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"  # Base URL for the NOAA CDO API endpoint.

# # Define parameters for the API request.
#     params = {
#         "datasetid": "GHCND",           # The dataset to query: 'GHCND' stands for Global Historical Climatology Network - Daily.
#         "locationid": "CITY:US170006",  # The location identifier for Chicago. 'CITY:US170006' represents the Chicago city code.
#         "datatypeid": "TAVG",           # The data type ID for average temperature (TAVG).
#         "startdate": "2000-01-01",      # Start date for the query (January 1, 2000).
#         "enddate": "2000-01-10",        # End date for the query (January 10, 2000).
#         "units": "metric",              # Units for temperature data. 'metric' means the temperatures will be in Celsius.
#         "limit": 25                     # Maximum number of results to return in the response.
#     }

# # Set headers with the API key for authorization.
#     headers = {
#         "token": API_KEY  # Pass the API key as a header to authenticate the request.
#     }

# # Make the GET request to the NOAA API endpoint.
#     response = requests.get(BASE_URL, headers=headers, params=params)

# # Check the response's status code to see if the request was successful.
#     if response.status_code == 200:  # Status code 200 indicates a successful response.
#         data = response.json()  # Parse the JSON response into a Python dictionary.
#         print(data)  # Print the response data to the console for inspection.
#         if "historical" in data and date_str in data["historical"]:
#             cache[date_str] = data["historical"][date_str]
#         else:
#             print(f"No data for {date_str}")
#     else:
#         print(f"Failed to fetch data for {date_str}: {response.status_code}")
#     # Save cache back to file
#     with open(cache_file, "w") as f:
#         json.dump(cache, f)

#         return cache

# get_weather_data()

# import requests
# import json

# # NOAA API details
# API_KEY = "WsrAbyxadIzZgtFanYBiXgzRcjaswpuH"  # Replace with your actual API key.
# BASE_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
# CACHE_FILE = "weather_cache.json"  # File to store cached data


# def fetch_and_cache_weather_data(start_date, end_date, location_id="CITY:US170006", limit=25):
#     """
#     Fetch weather data for a specific date range directly from the API and save it to the cache.
#     """
#     offset = 0  # Start at the first record
#     total_records_fetched = 0

#     while True:
#         # Define API request parameters
#         params = {
#             "datasetid": "GHCND",
#             "locationid": location_id,
#             "datatypeid": "TAVG",
#             "startdate": start_date,
#             "enddate": end_date,
#             "units": "metric",
#             "limit": limit,
#             "offset": offset
#         }

#         # Set headers with API key
#         headers = {"token": API_KEY}

#         # Make the API request
#         response = requests.get(BASE_URL, headers=headers, params=params)

#         if response.status_code == 200:
#             data = response.json()
#             print(f'HERE: {data}')

#             # Extract results
#             results = data.get("results", [])
#             if not results:
#                 print(f"No more data available for {start_date} to {end_date}.")
#                 break

#             # Add the fetched data to the cache
#             write_to_cache_file(results)

#             print(f"Fetched and cached {len(results)} records for {start_date} to {end_date}.")
#             total_records_fetched += len(results)

#             # Increment offset for the next batch
#             offset += limit
#         else:
#             print(f"Error fetching data for {start_date} to {end_date}: {response.status_code}, {response.text}")
#             break

#     print(f"Total records fetched and cached for {start_date} to {end_date}: {total_records_fetched}")


# def write_to_cache_file(new_data, cache_file=CACHE_FILE):
#     """
#     Write data to the cache file without overwriting existing content.
#     If the file doesn't exist, it will be created.
#     """
#     try:
#         # Step 1: Read existing content if the file exists
#         with open(cache_file, "r") as file:
#             existing_data = [json.loads(line) for line in file]
#     except FileNotFoundError:
#         existing_data = []

#     # Step 2: Append new data to the existing content
#     updated_data = existing_data + new_data

#     # Step 3: Write the updated content back to the file in "write" mode
#     with open(cache_file, "w") as file:
#         for record in updated_data:
#             file.write(json.dumps(record) + "\n")

#     print(f"Cache updated with {len(new_data)} new records.")


# def fetch_weather_for_multiple_ranges(years, cache_file=CACHE_FILE):
#     """
#     Fetch data for the first 10 days of each month in the specified years directly from the API.
#     """
#     for year in years:
#         for month in range(1, 13):  # Iterate through all 12 months
#             # Format the start and end dates for the first 10 days of the month
#             start_date = f"{year}-{month:02d}-01"  # YYYY-MM-DD format
#             end_date = f"{year}-{month:02d}-10"   # YYYY-MM-DD format

#             print(f"Fetching data for {start_date} to {end_date}...")
#             fetch_and_cache_weather_data(start_date, end_date, cache_file)


# # Example usage: Fetch for the first 10 days of each month in 2000 and 2020
# fetch_weather_for_multiple_ranges([2000, 2020])

import time
import requests
import json

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
    If the file doesn't exist, it will be created.
    """
    try:
        # Step 1: Read existing content if the file exists
        with open(cache_file, "r") as file:
            existing_data = [json.loads(line) for line in file]
    except FileNotFoundError:
        existing_data = []

    # Step 2: Append new data to the existing content
    updated_data = existing_data + new_data

    # Step 3: Write the updated content back to the file in "write" mode
    with open(cache_file, "w") as file:
        for record in updated_data:
            file.write(json.dumps(record) + "\n")

    print(f"Cache updated with {len(new_data)} new records.")


# def fetch_weather_for_multiple_ranges(years, cache_file=CACHE_FILE):
#     """
#     Fetch data for the first 10 days of each month in the specified years directly from the API.
#     """
#     for year in years:
#         for month in range(1, 13):  # Iterate through all 12 months
#             # Format the start and end dates for the first 10 days of the month
#             start_date = f"{year}-{month:02d}-01"  # YYYY-MM-DD format
#             end_date = f"{year}-{month:02d}-10"   # YYYY-MM-DD format

#             print(f"Fetching data for {start_date} to {end_date}...")
#             fetch_and_cache_weather_data(start_date, end_date, cache_file)


# Example usage: Fetch for the first 10 days of each month in 2000 and 2020
# fetch_weather_for_multiple_ranges([2000])
# fetch_and_cache_weather_data("2000-01-01", "2000-01-10", cache_file)
fetch_and_cache_weather_data("2000-01-01", "2000-01-10", location_id="CITY:US170006", limit=25)