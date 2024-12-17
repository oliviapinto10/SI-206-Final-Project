import requests
import sqlite3
import time
from datetime import datetime, timedelta

API_KEY = "WsrAbyxadIzZgtFanYBiXgzRcjaswpuH"
BASE_URL = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
STATION_ID = "GHCND:USW00094846"

def fetch_temperature_data(start_date, end_date):
    params = {
        "datasetid": "GHCND",
        "stationid": STATION_ID,
        "startdate": start_date,
        "enddate": end_date,
        "limit": 1000,
        "units": "metric",
        "datatypeid": "TMAX"
    }
    headers = {"token": API_KEY}
    try:
        response = requests.get(BASE_URL, params=params, headers=headers)
        response.raise_for_status()
        return response.json().get('results', [])
    except requests.RequestException as e:
        print(f"API request failed: {e}")
        return []

def create_database():
    conn = sqlite3.connect("Windy City Trends.db")
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS temperature_data
                 (id INTEGER PRIMARY KEY, date INTEGER UNIQUE, temperature REAL)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS last_processed_date
                     (id INTEGER PRIMARY KEY, date TEXT)''')
    return conn, cursor

def get_last_processed_date(cursor):
    cursor.execute("SELECT date FROM last_processed_date WHERE id = 1")
    result = cursor.fetchone()
    return datetime.strptime(result[0], "%Y-%m-%d") if result else datetime(2000, 1, 1)

def update_last_processed_date(cursor, date):
    cursor.execute("INSERT OR REPLACE INTO last_processed_date (id, date) VALUES (1, ?)", (date.strftime("%Y-%m-%d"),))

def get_next_valid_date(date):
    if date.year == 2000 and date.month == 12 and date.day > 5:
        return datetime(2020, 1, 1)
    elif date.year == 2020 and date.month == 12 and date.day > 5:
        return None
    elif date.day > 5:
        return datetime(date.year, date.month + 1, 1)
    else:
        return date

def main():
    conn, cursor = create_database()
    last_date = get_last_processed_date(cursor)
    last_date = get_next_valid_date(last_date)
    data_count = 0

    while last_date and data_count < 25:
        start_date = last_date
        end_date = min(start_date + timedelta(days=4), datetime(start_date.year, start_date.month, 5))

        data = fetch_temperature_data(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))

        for item in data:
            if data_count >= 25:
                break

            date = datetime.strptime(item.get('date'), "%Y-%m-%dT%H:%M:%S")
            temperature = item.get('value')

            date_int = int(date.strftime("%Y%m%d"))

            if date.date() >= last_date.date() and temperature is not None:
                cursor.execute("INSERT OR IGNORE INTO temperature_data (date, temperature) VALUES (?, ?)", (date_int, temperature))
                data_count += 1
                last_date = date
                print(f"Inserted data point {data_count}: {date_int}, {temperature}°C")

        conn.commit()
        update_last_processed_date(cursor, last_date)
        last_date = get_next_valid_date(end_date + timedelta(days=1))
        time.sleep(1)

    conn.close()
    print(f"Data collection complete. Total data points added: {data_count}")

if __name__ == "__main__":
    main()