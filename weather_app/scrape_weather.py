import sys
import os
from bs4 import BeautifulSoup
from urllib.request import urlopen, Request
from datetime import date
from urllib.error import HTTPError, URLError
from concurrent.futures import ThreadPoolExecutor, as_completed
from database.db_operations import DBOperations
sys.path.append(os.path.abspath('C:/Term 5/Python/Final Project'))
from database.db_operations import DBOperations
import calendar



class WeatherScraper:
    def __init__(self, start_date, end_date=date.today()):
        self.base_url = "https://climate.weather.gc.ca/climate_data/daily_data_e.html?StationID=27174&timeframe=2"
        self.start_date = start_date
        self.end_date = end_date
        self.weather_data = {}
        # Flag to indicate when to stop scraping
        self.no_more_data = False  

    def scrape_weather_data(self, year, month):
        # Check if the previous call found no more data
        if self.no_more_data:  
            return

        # Determine the number of days in the month
        _, num_days = calendar.monthrange(year, month)

        monthly_weather_data = {}
        try:
            url = f"{self.base_url}&Year={year}&Month={month}&Day=1"
            req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            response = urlopen(req)
            html = response.read().decode('utf-8')

            soup = BeautifulSoup(html, 'html.parser')
            table = soup.find("table", {"class": "table-striped"})
            if table:
                # Fetch only the rows for the actual days in the month
                rows = table.find_all("tr")[1:num_days+1]  
                day = 1
                for row in rows:
                    cols = row.find_all("td")
                    if cols and len(cols) > 3:
                        try:
                            max_temp = float(cols[0].text.strip())
                            min_temp = float(cols[1].text.strip())
                            mean_temp = float(cols[2].text.strip())
                            date_str = f"{year}-{month:02d}-{day:02d}"
                            monthly_weather_data[date_str] = {
                                "Max": max_temp,
                                "Min": min_temp,
                                "Mean": mean_temp
                            }
                            day += 1
                        except ValueError:
                            continue
            else:
                # Set flag if table is not found
                self.no_more_data = True  
                return

            self.weather_data.update(monthly_weather_data)
        except (HTTPError, URLError) as e:
            self.no_more_data = True 
            print(f"Error for {year}-{month}: {e}")

    def run(self):
    # Start from today's date
        current_year = date.today().year  
        current_month = date.today().month  
        start_year = self.start_date.year

        with ThreadPoolExecutor(max_workers=50) as executor:
            while current_year >= start_year and not self.no_more_data:
                futures = []
                # Start from current_month, go backwards
                for month in range(current_month, 0, -1):  
                    future = executor.submit(self.scrape_weather_data, current_year, month)
                    futures.append(future)

                # Wait for all futures of the current year to complete
                for future in as_completed(futures):
                    if self.no_more_data:
                        print("No more data available. Stopping.")
                        return

                # After completing a year, move to December of the previous year
                current_year -= 1
                # Reset month to December for the next iteration
                current_month = 12
    
    def get_weather_data(self):
        return self.weather_data    

if __name__ == "__main__":
    today = date.today()
    # Scrape from as far back as possible
    start_date = date(1900, 1, 1)  
    scraper = WeatherScraper(start_date)
    scraper.run()
    weather_data = scraper.get_weather_data()
    
    # Initialize DBOperations
    db_ops = DBOperations()
    
    # Location of scraped data: Winnipeg
    location_name = "Winnipeg"
    
    # Iterate over scraped weather data and save it to the database
    for date_str, temps in weather_data.items():
        db_ops.save_data(date_str, location_name, temps["Max"], temps["Min"], temps["Mean"])
    
    print("Weather data for Winnipeg has been saved to the database.")
