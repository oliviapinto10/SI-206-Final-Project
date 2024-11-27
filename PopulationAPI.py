import requests
import json

def get_weather_data_for_date(date_str, cache_file="weather_cache.json"):
    try:
        # Load existing cache
        with open(cache_file, "r") as f:
            cache = json.load(f)
    except FileNotFoundError:
        cache = {}

    api_key = "708e7032d77774eb38fa7db6c7635ce1" # Replace with your actual API key
    url = "https://api.weatherstack.com/historical"

    if date_str in cache:
        print(f"Data for {date_str} retrieved from cache.")
        return cache[date_str]  # Return cached data if available

    # Fetch data from the API
    querystring = {
        "access_key": api_key,
        "query": "Chicago",
        "historical_date": date_str,
        "hourly": 1
    }
    response = requests.get(url, params=querystring)

    if response.status_code == 200:
        # print(response.status_code)
        data = response.json()
        print(data)
        if "historical" in data and date_str in data["historical"]:
            cache[date_str] = data["historical"][date_str]  # Cache the result

            # Save updated cache back to the file
            with open(cache_file, "w") as f:
                json.dump(cache, f)

            print(f"Data for {date_str} fetched from the API.")
            return cache[date_str]
        else:
            print(f"No data available for {date_str}")
            return None
    else:
        print(f"Failed to fetch data for {date_str}: {response.status_code}")
        return None


date_str = "2020-01-01"  # Example: January 1, 2020
data = get_weather_data_for_date(date_str)

if data:
    print(f"Data for {date_str}: {data}")
else:
    print(f"No weather data found for {date_str}.")

