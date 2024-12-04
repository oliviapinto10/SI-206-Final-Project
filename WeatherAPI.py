import time
import requests
import json
import sqlite3
import os


API_KEY = "WsrAbyxadIzZgtFanYBiXgzRcjaswpuH" 
BASE_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
CACHE_FILE = "weather_cache.json" 


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
            return response.json()  
        else:
            print(f"Error: {response.status_code}, {response.text}")
            return None

 
    print("Max retries reached. Please try again later.")
    return None


def fetch_and_cache_weather_data(start_date, end_date, location_id="CITY:US170006", limit=25):
    """
    Fetch weather data for a specific date range directly from the API and save it to the cache.
    """
    offset = 0  
    total_records_fetched = 0

    while True:
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

        headers = {"token": API_KEY}

        data = fetch_with_retry(BASE_URL, headers, params)
       
        if data:
            results = data.get("results", [])
            if not results:
                print(f"No more data available for {start_date} to {end_date}.")
                break


            write_to_cache_file(results)

            print(f"Fetched and cached {len(results)} records for {start_date} to {end_date}.")
            total_records_fetched += len(results)

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
        with open(cache_file, "r") as file:
            existing_data = [json.loads(line) for line in file]
    except FileNotFoundError:
        existing_data = []

    data_by_date = {record["date"]: record for record in existing_data}
    
    for record in new_data:
        data_by_date[record["date"]] = record

    with open(cache_file, "w") as file:
        for record in data_by_date.values():
            file.write(json.dumps(record) + "\n")

    print(f"Cache updated with {len(new_data)} new records.")


def fetch_weather_for_years(years, days=10, location_id="CITY:US170006", limit=25):
    """
    Fetch weather data for the first 'days' days of every month for the specified years.
    """
    for year in years:
        for month in range(1, 13):  
            month_str = f"{month:02d}"
            
            start_date = f"{year}-{month_str}-01"
            end_date = f"{year}-{month_str}-{days:02d}"
            
            print(f"Fetching weather data for {start_date} to {end_date}...")
            fetch_and_cache_weather_data(start_date, end_date, location_id, limit)

fetch_weather_for_years([2000, 2020], days=10, location_id="CITY:US170006", limit=25)


def read_data_from_file(filename):
    """
    Reads data from a file with the given filename.

    Parameters
    -----------------------
    filename: str
        The name of the file to read.

    Returns
    -----------------------
    dict:
        Parsed JSON data from the file.
    """
    full_path = os.path.join(os.path.dirname(__file__), filename)
    f = open(full_path)
    file_data = f.read()
    f.close()
    json_data = json.loads(file_data)
    return json_data

db_name = 'Windy City Trends.db'
def set_up_database(db_name):
    """
    Sets up a SQLite database connection and cursor.

    Parameters
    -----------------------
    db_name: str
        The name of the SQLite database.

    Returns
    -----------------------
    Tuple (Cursor, Connection):
        A tuple containing the database cursor and connection objects.
    """
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + 'Windy City Trends.db')
    cur = conn.cursor()
    return cur, conn


def create_weather_table(cur, conn):
    """
    Creates the WeatherData table.
    """
    cur.execute("""
        CREATE TABLE IF NOT EXISTS WeatherData (
            id INTEGER PRIMARY KEY,
            date TEXT,
            value REAL
        )
    """)
    conn.commit()

def populate_weather_table(json_file, cur, conn, batch_size=25):
    """
    Populates the WeatherData table with data from a JSON file.
    """
    # with open(json_file, 'r') as f:
    #     data = [json.loads(line) for line in f.readlines()]

    # for entry in data:
    #     cur.execute("""
    #         INSERT INTO WeatherData (date, value)
    #         VALUES (?, ?)
    #     """, (entry['date'], entry['value']))
    
    # conn.commit()
    with open(json_file, 'r') as f:
        data = [json.loads(line) for line in f.readlines()]

    batch = []
    for i, entry in enumerate(data):
        batch.append((entry['date'], entry['value']))
        if len(batch) == batch_size or i == len(data) - 1:
            cur.executemany("""
                INSERT INTO WeatherData (date, value)
                VALUES (?, ?)
            """, batch)
            conn.commit()
            print(f"Inserted {len(batch)} records into the database.")
            batch = []

def main():
    cur, conn = set_up_database('db_name')
    create_weather_table(cur, conn)
    populate_weather_table('weather_cache.json', cur, conn, batch_size=25)
    conn.close()

if __name__ == "__main__":
    main()
