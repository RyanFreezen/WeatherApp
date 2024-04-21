import sys
import os
import calendar
from urllib.error import HTTPError, URLError
from urllib.request import urlopen, Request
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup
sys.path.append(os.path.abspath('C:/Term 5/Python/Final Project'))
from database.db_operations import DBOperations

class WeatherScraper:
    def __init__(self, start_date, end_date=date.today()):
        """
        Function to initialize the WeatherScraper class.
        """
        self.base_url = "https://climate.weather.gc.ca/climate_data/daily_data_e.html?StationID=27174&timeframe=2"
        self.start_date = start_date
        self.end_date = end_date
        self.weather_data = {}
        self.no_more_data = False

    def scrape_weather_data(self, year, month):
        """
        Function to scrape all weather data from the html tables on the webpages.
        """
        if self.no_more_data or date(year, month, calendar.monthrange
                                     (year, month)[1]) > self.end_date:
            return

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
                # Only fetch rows for actual days in the month
                rows = table.find_all("tr")[1:num_days + 1]
                for day, row in enumerate(rows, start=1):
                    cols = row.find_all("td")
                    # Ensure there are enough columns to parse
                    if cols and len(cols) >= 3:
                        try:
                            max_temp = float(cols[0].text.strip()) if cols[0].text.strip() else 0
                            min_temp = float(cols[1].text.strip()) if cols[1].text.strip() else 0
                            mean_temp = float(cols[2].text.strip()) if cols[2].text.strip() else 0
                        except ValueError:
                            max_temp, min_temp, mean_temp = 0, 0, 0
                        date_str = f"{year}-{month:02d}-{day:02d}"
                        monthly_weather_data[date_str] = {
                            "Max": max_temp,
                            "Min": min_temp,
                            "Mean": mean_temp
                        }
            else:
                self.no_more_data = True
                return

            self.weather_data.update(monthly_weather_data)
        except (HTTPError, URLError) as e:
            self.no_more_data = True
            print(f"Error for {year}-{month}: {e}")

    def run(self):
        """
        Function to speed up the scraping process.
        """
        current_date = self.end_date
        start_year, start_month = self.start_date.year, self.start_date.month

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []

            # Iterate from the end_date back to the start_date
            while current_date >= self.start_date:
                year = current_date.year
                month = current_date.month

                # Submit the task
                future = executor.submit(self.scrape_weather_data, year, month)
                futures.append(future)

                # Move to the previous month
                if month == 1:
                    current_date = current_date.replace(year=year - 1, month=12)
                else:
                    current_date = current_date.replace(month=month - 1)

        # Wait for all futures to complete
        for future in as_completed(futures):
            if self.no_more_data:
                print("No more data available. Please wait...")
                break

    def get_weather_data(self):
        """
        Function to return all weather data.
        """
        return self.weather_data

if __name__ == "__main__":
    # Start from as far back as possible
    start_date = date(1840, 1, 1)
    scraper = WeatherScraper(start_date)
    scraper.run()
    weather_data = scraper.get_weather_data()
    db_ops = DBOperations()
    location_name = "Winnipeg"

    for date_str, temps in weather_data.items():
        db_ops.save_data(date_str, location_name, temps["Max"], temps["Min"], temps["Mean"])
    print("Weather data for Winnipeg has been saved to the database.")