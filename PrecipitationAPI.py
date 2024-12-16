import requests
import sqlite3
from datetime import datetime


def get_date_range(year, month):
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-10"
    return start_date, end_date


def fetch_precipitation_data(offset=0, limit=25):
    latitude = 41.85
    longitude = -87.65

    years = [2000, 2020]
    months = range(1, 13)

    base_url = "https://archive-api.open-meteo.com/v1/era5"
    precipitation_data = []
    count = 0

    for year in years:
        for month in months:
            if count >= offset + limit:
                break

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
                time_series = data.get("daily", {}).get("time", [])
                precipitation_series = data.get("daily", {}).get("precipitation_sum", [])

                fetched_data = [
                    {"date": time_series[i], "value": precipitation_series[i]}
                    for i in range(len(time_series))
                ]

                for record in fetched_data:
                    if count >= offset:
                        # Convert the date string to integer YYYYMMDD format
                        date_obj = datetime.strptime(record["date"], "%Y-%m-%d")
                        record["date"] = int(date_obj.strftime("%Y%m%d"))
                        precipitation_data.append(record)
                    count += 1

                    if len(precipitation_data) >= limit:
                        break

            else:
                print(f"Failed to fetch data for {start_date} to {end_date}: {response.status_code}")

            if len(precipitation_data) >= limit:
                break

    return precipitation_data


current_offset = 0
db_file = 'Windy City Trends.db'
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS precipitation_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date INTEGER NOT NULL UNIQUE,
        value REAL NOT NULL
    );
""")

cursor.execute("SELECT COUNT(*) FROM precipitation_data")
current_offset = cursor.fetchone()[0]

precipitation_data = fetch_precipitation_data(offset=current_offset, limit=25)

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