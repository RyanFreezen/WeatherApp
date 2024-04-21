import datetime
import qprompt
from weather_app.scrape_weather import WeatherScraper
from database.db_operations import DBOperations
from data.plot_operations import PlotOperations
from database.dbcm import DBCM

class WeatherProcessor:
    def __init__(self):
        self.db_operations = DBOperations()
        self.plot_operations = PlotOperations()

    def start(self):
        menu = qprompt.Menu()
        menu.add("1", "Download Full Weather Data", self.download_all_data)
        menu.add("2", "Update Weather Data", self.update_data)
        menu.add("3", "Generate Box Plot", self.generate_box_plot)
        menu.add("4", "Generate Line Plot", self.generate_line_plot)

        menu.add("6", "Exit")

        while True:
            choice = menu.show(header="Weather Data Processor", returns="desc")
            if choice == "Exit":
                print("Exiting...")
                break

    def download_all_data(self):
        """
        Function to download all the weather data.
        """
        print("Downloading full weather data...")
        start_date = datetime.date(1840, 1, 1)
        end_date = datetime.date.today()
        scraper = WeatherScraper(start_date, end_date)
        scraper.run()
        weather_data = scraper.get_weather_data()

        for date_str, temps in weather_data.items():
            self.db_operations.save_data(date_str, "Winnipeg",
                                         temps["Max"], temps["Min"], temps["Mean"])
        print("Full weather data download complete.")

    def update_data(self):
        """
        Function to update weather data.
        """
        print("Updating weather data...")
        latest_date = self.db_operations.get_latest_date()
        if not latest_date:
            print("No existing data found. Consider downloading the full data set.")
            return

        # Assuming latest_date is a string in "YYYY-MM-DD" format.
        latest_date = datetime.datetime.strptime(latest_date, "%Y-%m-%d").date()

        start_date = latest_date + datetime.timedelta(days=1)
        end_date = datetime.date.today()

        if start_date > end_date:
            print("Data is already up to date.")
            return

        scraper = WeatherScraper(start_date, end_date)
        scraper.run()
        weather_data = scraper.get_weather_data()

        for date_str, temps in weather_data.items():
            self.db_operations.save_data(date_str, "Winnipeg", 
                                        temps["Max"], temps["Min"], temps["Mean"])
        print("Weather data update complete.")

    def generate_box_plot(self):
        """
        Function to generate a box plot of average temperatures based 
        on the start and end year given from user.
        """
        start_year = qprompt.ask_str("Enter start year:")
        end_year = qprompt.ask_str("Enter end year:")
        self.plot_operations.create_yearly_boxplot(int(start_year), int(end_year))

    def generate_line_plot(self):
        """
        Function to generate a line plot of average temperatures based 
        on the year and month given from user.
        """
        year = qprompt.ask_str("Enter year:")
        month = qprompt.ask_str("Enter month (1-12):")
        try:
            year_int = int(year)
            month_int = int(month)
            if month_int < 1 or month_int > 12:
                print("Error: Month must be between 1 and 12.")
                return
            if year_int < 1840 or year_int > datetime.date.today().year:
                print(f"Error: Year must be between 1840 and {datetime.date.today().year}.")
                return
        except ValueError:
            print("Error: Invalid input. Please enter numeric values.")
            return
        self.plot_operations.create_monthly_line_plot(year_int, month_int)

if __name__ == "__main__":
    processor = WeatherProcessor()
    processor.start()