import sqlite3
import matplotlib.pyplot as plt

#Population Visualization 

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
colors = ["pink", "violet", "red"]
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

#Precipitation Visualization  

def fetch_precipitation_from_db(db_name="Windy City Trends.db"):
    """Fetch all precipitation data from the database."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT date, value
        FROM precipitation_data
        ORDER BY date
    """)
    data = cursor.fetchall()
    conn.close()
    return data

precipitation_data = fetch_precipitation_from_db()

precipitation_by_year_month = {}
for date, value in precipitation_data:
    year, month, _ = date.split("-")
    if year not in precipitation_by_year_month:
        precipitation_by_year_month[year] = {str(i).zfill(2): 0 for i in range(1, 13)}
    precipitation_by_year_month[year][month] += value

months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

colors = ["navy", "darkorange"]  

plt.figure(figsize=(10, 6))

for i, (year, monthly_data) in enumerate(precipitation_by_year_month.items()):
    monthly_precipitation = [monthly_data[str(i).zfill(2)] for i in range(1, 13)]
    plt.plot(months, monthly_precipitation, marker='o', label=f"Year {year}", color=colors[i % len(colors)])

plt.title("Monthly Precipitation in Chicago (2000 & 2020)", fontsize=17)
plt.xlabel("Month", fontsize=15)
plt.ylabel("Total Precipitation (mm)", fontsize=15)
plt.xticks(months, fontsize=11)
plt.legend(title="Year Legend", fontsize=11)

plt.show() 