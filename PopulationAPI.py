import sqlite3
import requests
from datetime import datetime

def initialize_database(db_name="Windy City Trends.db"):
    """Initialize the SQLite database and create the table if it doesn't exist."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS chicago_population (
            date INTEGER PRIMARY KEY,
            population INTEGER
        )
    """)
    conn.commit()
    conn.close()

def fetch_population_from_db(db_name, date):
    """Fetch population data for a specific date from the database."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("SELECT population FROM chicago_population WHERE date = ?", (date,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

def save_population_to_db(db_name, data):
    """Save population data to the database."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.executemany("""
        INSERT OR IGNORE INTO chicago_population (date, population) VALUES (?, ?)
    """, [(entry['date'], entry['population']) for entry in data])
    conn.commit()
    conn.close()

def fetch_chicago_population(api_key, db_name="Windy City Trends.db"):
    base_url = "https://api.census.gov/data"
    datasets = {
        2000: {"endpoint": "2000/dec/sf1", "variable": "P001001"},  
        2020: {"endpoint": "2020/dec/pl", "variable": "P1_001N"}   
    }
    chicago_geo_code = {"state": "17", "place": "14000"}  

    years = [2000, 2020]
    first_5_days = [f"{month:02d}-{day:02d}" for month in range(1, 13) for day in range(1, 6)]

    results = []

    for year in years:
        dataset = datasets[year]

        url = f"{base_url}/{dataset['endpoint']}"
        params = {
            "get": dataset["variable"],  
            "for": f"place:{chicago_geo_code['place']}",
            "in": f"state:{chicago_geo_code['state']}",
            "key": api_key
        }
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            population = int(data[1][0])
            
            for date in first_5_days:
                date_str = f"{year}-{date}"

                # Convert date string to integer in YYYYMMDD format
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                date_int = int(date_obj.strftime("%Y%m%d"))

                if not fetch_population_from_db(db_name, date_int):
                    results.append({
                        "date": date_int,
                        "population": population
                    })

                    if len(results) >= 25:
                        break
        else:
            print(f"Failed to fetch data for {year}: {response.status_code} {response.text}")
            return None

        if len(results) >= 25:
            break

    save_population_to_db(db_name, results)

    return results

initialize_database()

api_key = "b7a8a236fca72ebb6fa078d92b400cdd5a406a90"
population_data = fetch_chicago_population(api_key)

if population_data:
    for entry in population_data:
        print(f"Date: {entry['date']}, Population: {entry['population']}")