import sys
import os
sys.path.append(os.path.abspath('C:/Term 5/Python/Final Project'))
from database.db_operations import DBOperations 
import matplotlib.pyplot as plt
 
class PlotOperations:
    def __init__(self, db_name="weather_data.db"):
        self.db_operations = DBOperations(db_name)

    def create_yearly_boxplot(self, start_year, end_year):
        data = self.db_operations.fetch_box_data(start_year, end_year)
        if not data:
            print("No data available to plot.")
            return

        months = [str(month).zfill(2) for month in range(1, 13)]
        temperatures = [data[month] for month in months if data[month] is not None]  # Exclude None values for plotting

        if not temperatures:  # Check if there's any data to plot
            print("No temperature data available to plot.")
            return

        plt.figure(figsize=(12, 6))
        # Convert each temperature to a list if not None, because boxplot expects a sequence
        temperatures = [[temp] if temp is not None else [] for temp in temperatures]
        plt.boxplot(temperatures, labels=months)
        plt.xlabel('Month')
        plt.ylabel('Mean Temperature (°C)')
        plt.title(f'Box Plot of Mean Temperatures from {start_year} to {end_year}')
        plt.grid(True)
        plt.show()

    def create_monthly_line_plot(self, year, month):
        data = self.db_operations.fetch_line_data(year, month)
        if not data:
            print("No data available to plot.")
            return

        days = [d[0] for d in data]  # Days of the month
        temperatures = [d[1] for d in data]  # Daily mean temperatures

        plt.figure(figsize=(10, 5))
        plt.plot(days, temperatures, marker='o', linestyle='-')
        plt.xlabel('Day')
        plt.ylabel('Temperature (°C)')
        plt.title(f'Daily Mean Temperatures for {str(month).zfill(2)}/{year}')
        plt.grid(True)
        plt.xticks(days, rotation=45)  # Ensure the days are used as x-ticks
        plt.tight_layout()
        plt.show()



if __name__ == "__main__":
    plot_ops = PlotOperations()