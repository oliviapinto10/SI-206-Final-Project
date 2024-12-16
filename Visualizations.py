import sqlite3
import matplotlib.pyplot as plt

#Population Visualization (bar graph)

def fetch_population_for_years(db_name="Windy City Trends.db"):
    """Fetch population for the years 2000 and 2020 from the database."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT SUBSTR(date, 1, 4) AS year, AVG(population) AS avg_population
        FROM chicago_population
        WHERE year IN ('2000', '2020')
        GROUP BY year
        ORDER BY year
    """)
    data = cursor.fetchall()
    conn.close()
    return {row[0]: row[1] for row in data}

population_data = fetch_population_for_years()  

population_2000 = population_data.get("2000", 0)
population_2020 = population_data.get("2020", 0)
population_change = population_2020 - population_2000

categories = ["2000", "2020", "Change"]
values = [population_2000, population_2020, population_change]

plt.figure(figsize=(8, 6))
colors = ["gray", "teal", "red"]
bars = plt.bar(categories, values, color=colors)

for i, bar in enumerate(bars):
    height = bar.get_height()
    text_color = "white" if categories[i] == "Change" else "black"
    plt.text(bar.get_x() + bar.get_width() / 2, height, f"{int(height):,}", 
             ha="center", va="bottom", color=text_color, fontsize=10)

plt.title("Population Comparison: Chicago (2000 vs 2020)", fontsize=15)
plt.ylabel("Population (millions)", fontsize=13)
plt.xlabel("Years", fontsize=13)

plt.show()

#Precipitation Visualization (line chart) 

def fetch_precipitation_from_db(db_name="Windy City Trends.db"):
    """Fetch all precipitation data from the database."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT date, value
        FROM precipitation_data
    """)
    data = cursor.fetchall()
    conn.close()
    return data

precipitation_data = fetch_precipitation_from_db()
precipitation_by_year_month = {} 

for date, value in precipitation_data:
    date_str = str(date)
    year, month, _ = date_str[:4], date_str[4:6], date_str[6:]
    
    if year not in precipitation_by_year_month:
        precipitation_by_year_month[year] = {str(i).zfill(2): [] for i in range(1, 13)}
    
    precipitation_by_year_month[year][month].append(value)

monthly_averages_by_year_precipitation = {}
for year, monthly_data in precipitation_by_year_month.items():
    monthly_averages = {month: sum(values) / len(values) for month, values in monthly_data.items()}
    monthly_averages_by_year_precipitation[year] = monthly_averages

months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
colors = ["navy", "coral"]

plt.figure(figsize=(10, 6))

for i, (year, monthly_data) in enumerate(monthly_averages_by_year_precipitation.items()):
    monthly_averages = [monthly_data[str(i).zfill(2)] for i in range(1, 13)]
    plt.plot(months, monthly_averages, marker='o', label=f"Year {year}", color=colors[i % len(colors)])

plt.title("Monthly Average Precipitation in Chicago (2000 & 2020)", fontsize=17)
plt.xlabel("Month", fontsize=15)
plt.ylabel("Average Precipitation (mm)", fontsize=15)
plt.xticks(fontsize=11)
plt.legend(title="Year Legend", fontsize=11)

plt.show()

# Humidity Visualization (line chart) 

def fetch_humidity_from_db(db_name="Windy City Trends.db"):
    """Fetch all humidity data from the database."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT date, humidity
        FROM humidity_data
    """)
    data = cursor.fetchall()
    conn.close()
    return data

humidity_data = fetch_humidity_from_db()

humidity_by_year_month = {}
for date, humidity in humidity_data:
    date_str = str(date)
    year, month, _ = date_str[:4], date_str[4:6], date_str[6:]
    
    if year not in humidity_by_year_month:
        humidity_by_year_month[year] = {str(i).zfill(2): [] for i in range(1, 13)}
    
    humidity_by_year_month[year][month].append(humidity)

monthly_averages_by_year = {}
for year, monthly_data in humidity_by_year_month.items():
    monthly_averages = {month: sum(humidity_values) / len(humidity_values) for month, humidity_values in monthly_data.items()}
    monthly_averages_by_year[year] = monthly_averages

months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

colors = ["green", "brown"]  

plt.figure(figsize=(10, 6))

for i, (year, monthly_data) in enumerate(monthly_averages_by_year.items()):
    monthly_averages = [monthly_data[str(i).zfill(2)] for i in range(1, 13)]
    plt.plot(months, monthly_averages, marker='o', label=f"Year {year}", color=colors[i % len(colors)])

plt.title("Monthly Average Humidity in Chicago (2000 & 2020)", fontsize=17)
plt.xlabel("Month", fontsize=15)
plt.ylabel("Average Humidity (%)", fontsize=15)
plt.xticks(fontsize=11)
plt.legend(title="Year Legend", fontsize=11)

plt.show()

# Temperature Visualization (line chart) 

def fetch_temperature_from_db(db_name="Windy City Trends.db"):
    """Fetch all temperature data from the database."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT date, temperature
        FROM temperature_data
    """)
    data = cursor.fetchall()
    conn.close()
    return data

temperature_data = fetch_temperature_from_db()

temperature_by_year_month = {}
for date, temperature in temperature_data:
    date = str(date)
    year = date[:4]   
    month = date[4:6] 

    if year not in temperature_by_year_month:
        temperature_by_year_month[year] = {str(i).zfill(2): [] for i in range(1, 13)}

    temperature_by_year_month[year][month].append(temperature)

monthly_averages_by_year = {}
for year, monthly_data in temperature_by_year_month.items():
    monthly_averages = {month: sum(temp_values) / len(temp_values) if temp_values else None 
                        for month, temp_values in monthly_data.items()}
    monthly_averages_by_year[year] = monthly_averages

months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
colors = ["blue", "magenta"]  

plt.figure(figsize=(10, 6))

for i, (year, monthly_data) in enumerate(monthly_averages_by_year.items()):
    monthly_averages = [monthly_data.get(str(i).zfill(2), None) for i in range(1, 13)]
    plt.plot(months, monthly_averages, marker='o', label=f"Year {year}", color=colors[i % len(colors)])

plt.title("Monthly Average Temperature in Chicago (2000 & 2020)", fontsize=17)
plt.xlabel("Month", fontsize=15)
plt.ylabel("Average Temperature (°C)", fontsize=15)
plt.xticks(months, fontsize=11)
plt.legend(title="Year Legend", fontsize=11)

plt.show()  


# Calculation File 

def save_combined_data_to_file(population_data, precipitation_data, humidity_data, temperature_data, filename="combined_calculation.txt"):
    """Save all data into one combined text file."""
    population_2000 = population_data.get("2000", 0)
    population_2020 = population_data.get("2020", 0)
    population_change = population_2020 - population_2000

    # Population Data
    with open(filename, "w") as file:
        file.write("Population Data:\n")
        file.write(f"2000: {population_2000:,} people\n")
        file.write(f"2020: {population_2020:,} people\n")
        file.write(f"Change: {population_change:,} people\n\n")

        # Precipitation Data
        file.write("Monthly Average Precipitation Data (in mm):\n\n")
        for year, monthly_data in precipitation_data.items():
            file.write(f"Year {year}:\n")
            for month, avg_value in monthly_data.items():
                file.write(f"  {month}: {avg_value:.2f} mm\n")
            file.write("\n")

        # Humidity Data
        file.write("Monthly Average Humidity Data (%):\n\n")
        for year, monthly_data in humidity_data.items():
            file.write(f"Year {year}:\n")
            for month, avg_humidity in monthly_data.items():
                file.write(f"  {month}: {avg_humidity:.2f}%\n")
            file.write("\n")

        # Temperature Data
        file.write("Monthly Average Temperature Data (°C):\n\n")
        for year, monthly_data in temperature_data.items():
            file.write(f"Year {year}:\n")
            for month, avg_temperature in monthly_data.items():
                if avg_temperature is not None:
                    file.write(f"  {month}: {avg_temperature:.2f} °C\n")
                else:
                    file.write(f"  {month}: No data\n")
            file.write("\n")

    print(f"All data combined and written to {filename}")

save_combined_data_to_file(
    population_data=population_data,
    precipitation_data=monthly_averages_by_year_precipitation,
    humidity_data=monthly_averages_by_year,
    temperature_data=monthly_averages_by_year
)

#JOIN with Population + Temperature and Visualization 

import numpy as np

def fetch_population_and_monthly_temperature(db_name="Windy City Trends.db"):
    """Fetch population and monthly temperature data for 2000 and 2020."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
   
    cursor.execute("""
        SELECT 
            SUBSTR(p.date, 1, 4) AS year,
            SUBSTR(p.date, 5, 2) AS month,
            AVG(p.population) AS avg_population,
            AVG(t.temperature) AS avg_temperature
        FROM chicago_population p
        INNER JOIN temperature_data t  -- Explicit INNER JOIN
        ON SUBSTR(p.date, 1, 6) = SUBSTR(t.date, 1, 6)  -- Match year and month
        WHERE SUBSTR(p.date, 1, 4) IN ('2000', '2020')
        GROUP BY year, month
        ORDER BY year, month;
    """)
    data = cursor.fetchall()
    conn.close()

    population_and_temperature = {"2000": {}, "2020": {}}
    for year, month, avg_population, avg_temperature in data:
        population_and_temperature[year][month] = {"avg_population": avg_population, "avg_temperature": avg_temperature}
    
    return population_and_temperature

def visualize_population_and_temperature(data):
    """Create a dual-axis chart with population and temperature by month."""
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    month_numbers = [str(i).zfill(2) for i in range(1, 13)]  
    
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    ax2 = ax1.twinx() 
    colors = {"2000": "purple", "2020": "gold"}
    
    for year, monthly_data in data.items():
        avg_population = next(iter(monthly_data.values()))["avg_population"] / 1e6  
        avg_temperatures = [monthly_data.get(month, {}).get("avg_temperature", None) for month in month_numbers]
       
        ax2.plot(months, avg_temperatures, marker='o', label=f"Temperature ({year})", color=colors[year], linewidth=2)

        ax1.hlines(avg_population, xmin=0, xmax=len(months)-1, colors=colors[year], label=f"Population ({year})", linewidth=3)  

    ax1.set_ylabel("Population (millions)", fontsize=13, color="black")
    ax1.tick_params(axis='y', labelcolor="black")

    ax2.set_ylabel("Temperature (°C)", fontsize=13, color="black")
    ax2.tick_params(axis='y', labelcolor="black")

    plt.title("Monthly Temperature vs. Population in Chicago (2000 & 2020)", fontsize=15)
    ax1.set_xlabel("Month", fontsize=13)
    ax1.set_xticks(range(len(months)))
    ax1.set_xticklabels(months, fontsize=11)

    ax1.legend(loc="upper left", fontsize=10)
    ax2.legend(loc="upper right", fontsize=10)

    plt.show()

data = fetch_population_and_monthly_temperature()
visualize_population_and_temperature(data)