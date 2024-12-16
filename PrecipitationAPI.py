import requests
import sqlite3
from datetime import datetime

def get_date_range(year, month, is_hourly=False):
    """Returns the start and end date for API queries."""
    if is_hourly:
        return f"{year}-{month:02d}-01", f"{year}-{month:02d}-01"  
    else:
        return f"{year}-{month:02d}-01", f"{year}-{month:02d}-10"

def create_database(db_file):
    """Creates and initializes the SQLite database with the tables."""
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute(""" 
        CREATE TABLE IF NOT EXISTS precipitation_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date INTEGER NOT NULL UNIQUE,
            value REAL NOT NULL
        );
    """)

    cursor.execute(""" 
        CREATE TABLE IF NOT EXISTS hourly_precipitation_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date INTEGER NOT NULL,
            hour INTEGER NOT NULL,
            value REAL NOT NULL,
            FOREIGN KEY (date) REFERENCES precipitation_data (date)
        );
    """)

    conn.commit()
    return conn, cursor

def fetch_precipitation_data(offset=0, limit=5, hourly=False):
    """Fetches precipitation data from the Open-Meteo API."""
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

            start_date, end_date = get_date_range(year, month, hourly)
            params = {
                "latitude": latitude,
                "longitude": longitude,
                "start_date": start_date,
                "end_date": end_date,
                "timezone": "America/Chicago"
            }
            if hourly:
                params["hourly"] = "precipitation" 
            else:
                params["daily"] = "precipitation_sum"

            print(f"Fetching data for {start_date}...")
            response = requests.get(base_url, params=params)
            if response.status_code == 200:
                data = response.json()
                if hourly:
                    time_series = data.get("hourly", {}).get("time", [])
                    precipitation_series = data.get("hourly", {}).get("precipitation", [])
                    fetched_data = [
                        {
                            "date": time_series[i], 
                            "hour": int(time_series[i].split("T")[1].split(":")[0]),
                            "value": precipitation_series[i]
                        }
                        for i in range(len(time_series))
                        if 12 <= int(time_series[i].split("T")[1].split(":")[0]) <= 23
                    ]
                else:
                    time_series = data.get("daily", {}).get("time", [])
                    precipitation_series = data.get("daily", {}).get("precipitation_sum", [])
                    fetched_data = [
                        {"date": time_series[i], "value": precipitation_series[i]}
                        for i in range(len(time_series))
                    ]

                for record in fetched_data:
                    if count >= offset:
                        if hourly:
                            date_obj = datetime(year, month, 1, 0, 0, 0) 
                        else:
                            date_obj = datetime.strptime(record["date"], "%Y-%m-%d")  
                        record["date"] = int(date_obj.strftime("%Y%m%d"))
                        precipitation_data.append(record)
                    count += 1

                    if len(precipitation_data) >= limit:
                        break

            else:
                print(f"Failed to fetch data for {start_date}: {response.status_code} - {response.text}")

            if len(precipitation_data) >= limit:
                break

    return precipitation_data

def insert_data(cursor, table_name, data, columns):
    """Inserts data into the specified table in the database."""
    placeholders = ", ".join(["?" for _ in columns])
    query = f"INSERT OR IGNORE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    cursor.executemany(query, data)

def process_data(db_file, batch_size_hourly=5, batch_size_daily=20, total_limit=100):
    """
    - Inserts a total of 25 records (5 hourly + 20 daily) per run.
    - After inserting 100 records, does a bulk insertion for the remaining data.
    """
    conn, cursor = create_database(db_file)
    total_records_inserted = 0

    
    cursor.execute("SELECT COUNT(*) FROM precipitation_data")
    daily_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM hourly_precipitation_data")
    hourly_count = cursor.fetchone()[0]
    total_records_inserted = daily_count + hourly_count

    if total_records_inserted < total_limit:
        print(f"Current total records: {total_records_inserted}. Adding 25 more records")

        remaining_limit = total_limit - total_records_inserted

    
        hourly_records_inserted = 0
        if remaining_limit > 0:
            cursor.execute("SELECT COUNT(*) FROM hourly_precipitation_data")
            current_offset_hourly = cursor.fetchone()[0]
            hourly_data = fetch_precipitation_data(
                offset=current_offset_hourly, 
                limit=min(batch_size_hourly, remaining_limit), 
                hourly=True
            )
            hourly_records = [(record["date"], record["hour"], record["value"]) for record in hourly_data]
            insert_data(cursor, "hourly_precipitation_data", hourly_records, ["date", "hour", "value"])
            hourly_records_inserted = len(hourly_records)
            total_records_inserted += hourly_records_inserted
            print(f"Inserted {hourly_records_inserted} hourly records. Total: {total_records_inserted}")

        
        daily_records_inserted = 0
        if remaining_limit > 0:
            cursor.execute("SELECT COUNT(*) FROM precipitation_data")
            current_offset_daily = cursor.fetchone()[0]
            daily_data = fetch_precipitation_data(
                offset=current_offset_daily, 
                limit=min(batch_size_daily, remaining_limit - hourly_records_inserted), 
                hourly=False
            )
            daily_records = [(record["date"], record["value"]) for record in daily_data]
            insert_data(cursor, "precipitation_data", daily_records, ["date", "value"])
            daily_records_inserted = len(daily_records)
            total_records_inserted += daily_records_inserted
            print(f"Inserted {daily_records_inserted} daily records. Total: {total_records_inserted}")

        
        conn.commit()
        print(f"Inserted a total of {hourly_records_inserted + daily_records_inserted} records this run.")
    else:
        print("100 records already inserted. Bulk inserting of remaining data...")

        
        while True:
            cursor.execute("SELECT COUNT(*) FROM precipitation_data")
            daily_offset = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM hourly_precipitation_data")
            hourly_offset = cursor.fetchone()[0]

            daily_data = fetch_precipitation_data(offset=daily_offset, limit=1000, hourly=False)
            hourly_data = fetch_precipitation_data(offset=hourly_offset, limit=1000, hourly=True)

            if not daily_data and not hourly_data:
                break  

            if daily_data:
                daily_records = [(record["date"], record["value"]) for record in daily_data]
                insert_data(cursor, "precipitation_data", daily_records, ["date", "value"])
                print(f"Final bulk run: Inserted {len(daily_records)} daily records.")

            if hourly_data:
                hourly_records = [(record["date"], record["hour"], record["value"]) for record in hourly_data]
                insert_data(cursor, "hourly_precipitation_data", hourly_records, ["date", "hour", "value"])
                print(f"Final bulk run: Inserted {len(hourly_records)} hourly records.")

            conn.commit()

    cursor.close()
    conn.close()
    print("Data insertion process completed.")



if __name__ == "__main__":
    process_data("Windy City Trends.db")