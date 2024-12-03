API_KEY = "openuv-7mi3osrm3qlevog-io"

import requests
import json
from datetime import datetime

# Set API key and base URL
API_KEY = "openuv-7mi3osrm3qlevog-io"
BASE_URL = "https://api.openuv.io/api/v1/uv"

# Location coordinates for Chicago
LAT = 41.8781
LON = -87.6298

# Function to fetch UV data for a specific date
def fetch_uv_data(date):
    headers = {"x-access-token": API_KEY}
    params = {"lat": LAT, "lng": LON, "dt": date}
    response = requests.get(BASE_URL, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None

# Generate dates for the first 10 days of each month in the given years
def generate_dates(years):
    dates = []
    for year in years:
        for month in range(1, 13):
            for day in range(1, 11):
                try:
                    date = datetime(year, month, day).isoformat() + "Z"
                    dates.append(date)
                except ValueError:
                    continue  # Skip invalid dates
    return dates

# Process and cache data in batches
def process_batches(dates, batch_size=25):
    max_uv_data = {}

    # Try to load existing cache if available
    try:
        with open("uv_data_cache.json", "r") as cache_file:
            max_uv_data = json.load(cache_file)
    except FileNotFoundError:
        print("No existing cache found. Starting fresh.")

    # Process data in batches
    for i in range(0, len(dates), batch_size):
        batch = dates[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1}: {batch[0]} to {batch[-1]}")

        for date in batch:
            if date in max_uv_data:  # Skip already cached data
                print(f"Data for {date} already cached. Skipping.")
                continue
            
            uv_data = fetch_uv_data(date)
            if uv_data:
                uv_max = uv_data.get("result", {}).get("uv_max", None)
                if uv_max is not None:
                    max_uv_data[date] = uv_max

        # Save to cache after each batch
        with open("uv_data_cache.json", "w") as cache_file:
            json.dump(max_uv_data, cache_file, indent=4)
        print(f"Batch {i//batch_size + 1} processed and cached!")

    print("All batches processed successfully!")

# Main function
def main():
    years = [2000, 2020]
    # dates = generate_dates(years)
    # process_batches(dates)
    process_batches("")

# Run the script
if __name__ == "__main__":
    main()

     
