import requests
import sqlite3

def get_date_range(year, month, days=5):  
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{days:02d}"  
    return start_date, end_date

def fetch_humidity_data(offset=0, limit=25):
    latitude = 41.85  
    longitude = -87.65

    years = [2000, 2020] 
    months = range(1, 13)  

    base_url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
    humidity_data = []
    count = 0

    for year in years:
        for month in months:
            if count >= offset + limit:
                break

            start_date, end_date = get_date_range(year, month)
            params = {
                "unitGroup": "metric",
                "include": "humidity",
                "key": "RNPVCKPGQP3VMSVR72GYY35WX",  
                "contentType": "json"
            }

            print(f"Fetching data for {start_date} to {end_date}...")
            response = requests.get(f"{base_url}/{latitude},{longitude}/{start_date}/{end_date}", params=params)

            if response.status_code == 200:
                data = response.json()
                if 'days' in data:
                    fetched_data = [
                        {"date": day["datetime"], "humidity": day.get("humidity", None)}
                        for day in data['days']
                    ]

                    for record in fetched_data:
                        if count >= offset:
                            humidity_data.append(record)
                        count += 1

                        if len(humidity_data) >= limit:
                            break

            else:
                print(f"Failed to fetch data for {start_date} to {end_date}: {response.status_code}")

            if len(humidity_data) >= limit:
                break

    return humidity_data

db_file = 'Windy City Trends.db'
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

cursor.execute("""
   CREATE TABLE IF NOT EXISTS humidity_data (
   	id INTEGER PRIMARY KEY AUTOINCREMENT,
   	date TEXT NOT NULL UNIQUE,
   	humidity REAL NOT NULL
   );
""")

cursor.execute("SELECT COUNT(*) FROM humidity_data")
current_offset = cursor.fetchone()[0]

humidity_data = fetch_humidity_data(offset=current_offset, limit=25)

insert_query = "INSERT OR IGNORE INTO humidity_data (date, humidity) VALUES (?, ?)"

records_to_insert = [(record["date"], record["humidity"]) for record in humidity_data]

batch_size = 25

for i in range(0, len(records_to_insert), batch_size):
    batch = records_to_insert[i:i + batch_size]
    cursor.executemany(insert_query, batch)

conn.commit()

cursor.execute("""
   DELETE FROM humidity_data
   WHERE id NOT IN (
   	SELECT MIN(id)
   	FROM humidity_data
   	GROUP BY date
   );
""")

conn.commit()

cursor.close()
conn.close()