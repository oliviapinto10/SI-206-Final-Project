
import requests
import json
import os
import sqlite3

def fetch_precipitation_data_with_cache(cache_file="precipitation_cache.json"):
    latitude = 41.85
    longitude = -87.65

    def get_date_range(year, month):
        start_date = f"{year}-{month:02d}-01"
        end_date = f"{year}-{month:02d}-10"
        return start_date, end_date

    years = [2000, 2020]
    months = range(1, 13)

    base_url = "https://archive-api.open-meteo.com/v1/era5"

    
    if os.path.exists(cache_file):
        with open(cache_file, "r") as file:
            cache = json.load(file)
    else:
        cache = {}

    
    precipitation_data = []

    for year in years:
        for month in months:
            cache_key = f"{year}-{month:02d}"
            if cache_key in cache:
                print(f"Data for {cache_key} found in cache.")
                
                precipitation_data.extend(cache[cache_key])
                continue

            start_date, end_date = get_date_range(year, month)
            params = {
                "latitude": latitude,
                "longitude": longitude,
                "start_date": start_date,
                "end_date": end_date,
                "daily": "precipitation_sum",
                "timezone": "America/Chicago"
            }

            print(f"Fetching data for {start_date} to {end_date}...")
            response = requests.get(base_url, params=params)

            if response.status_code == 200:
                data = response.json()
                print(data)
                time_series = data.get("daily", {}).get("time", [])
                precipitation_series = data.get("daily", {}).get("precipitation_sum", [])

                
                fetched_data = [{"date": time_series[i], "value": precipitation_series[i]} for i in range(len(time_series))]
                precipitation_data.extend(fetched_data)

               
                cache[cache_key] = fetched_data
            else:
                print(f"Failed to fetch data for {start_date} to {end_date}: {response.status_code}")

    with open(cache_file, "w") as file:
        json.dump(cache, file, indent=4)

    return precipitation_data

precipitation_data = fetch_precipitation_data_with_cache()

db_file = 'Windy City Trends.db'
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS precipitation_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL UNIQUE,
        value REAL NOT NULL
    );
""")

insert_query = "INSERT OR IGNORE INTO precipitation_data (date, value) VALUES (?, ?)"

records_to_insert = [(record["date"], record["value"]) for record in precipitation_data]

batch_size = 25

for i in range(0, len(records_to_insert), batch_size):
    batch = records_to_insert[i:i + batch_size]
    cursor.executemany(insert_query, batch)

conn.commit()

cursor.execute("""
    DELETE FROM precipitation_data
    WHERE id NOT IN (
        SELECT MIN(id)
        FROM precipitation_data
        GROUP BY date
    );
""")

conn.commit()


cursor.close()
conn.close()



