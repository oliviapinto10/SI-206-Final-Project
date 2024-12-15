import requests
import sqlite3
from datetime import datetime

def get_date_range(year, month, is_hourly=False):
    """
    Get start and end date for the query.
    For hourly data, return only the first day; for daily data, return the first 10 days.
    """
    if is_hourly:
        
        return f"{year}-{month:02d}-01", f"{year}-{month:02d}-01"  
    else:
        
        return f"{year}-{month:02d}-01", f"{year}-{month:02d}-10"

def fetch_precipitation_data(offset=0, limit=25, hourly=False):
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
                        {"date": time_series[i], "hour": int(time_series[i].split("T")[1].split(":")[0]), "value": precipitation_series[i]}
                        for i in range(len(time_series))
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

cursor.execute(""" 
    CREATE TABLE IF NOT EXISTS hourly_precipitation_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date INTEGER NOT NULL,
        hour INTEGER NOT NULL,
        value REAL NOT NULL,
        FOREIGN KEY (date) REFERENCES precipitation_data (date)
    );
""")


cursor.execute("SELECT COUNT(*) FROM precipitation_data")
current_offset = cursor.fetchone()[0]


precipitation_data = fetch_precipitation_data(offset=current_offset, limit=25, hourly=False)


insert_query = "INSERT OR IGNORE INTO precipitation_data (date, value) VALUES (?, ?)"
records_to_insert = [(record["date"], record["value"]) for record in precipitation_data]

batch_size = 25
for i in range(0, len(records_to_insert), batch_size):
    batch = records_to_insert[i:i + batch_size]
    cursor.executemany(insert_query, batch)
    print(f"Inserted {len(batch)} records into precipitation_data.")


cursor.execute("SELECT COUNT(*) FROM hourly_precipitation_data")
current_offset_hourly = cursor.fetchone()[0]


hourly_precipitation_data = fetch_precipitation_data(offset=current_offset_hourly, limit=25, hourly=True)


hourly_insert_query = "INSERT OR IGNORE INTO hourly_precipitation_data (date, hour, value) VALUES (?, ?, ?)"
hourly_records_to_insert = [(record["date"], record["hour"], record["value"]) for record in hourly_precipitation_data]

for i in range(0, len(hourly_records_to_insert), batch_size):
    batch = hourly_records_to_insert[i:i + batch_size]
    cursor.executemany(hourly_insert_query, batch)
    print(f"Inserted {len(batch)} records into hourly_precipitation_data.")


conn.commit()
cursor.close()
conn.close()

print("Data has been successfully inserted into the database.")
