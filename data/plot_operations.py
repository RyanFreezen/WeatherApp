import sys
import os
sys.path.append(os.path.abspath('C:/Term 5/Python/Final Project'))
from database.db_operations import DBOperations 
import matplotlib.pyplot as plt
 

class PlotOperations:
    def __init__(self, db_name="weather_data.db"):
        self.db_operations = DBOperations(db_name)

    def create_boxplot(self, start_year, end_year):
        """
        Create a boxplot for each month over a specified range of years.
        """
        data = self.db_operations.fetch_data(start_year=start_year, end_year=end_year)
        # Assume data is a dict where keys are months ('01', '02', ...) and values are lists of temperatures
        plt.boxplot([data[str(month).zfill(2)] for month in range(1, 13) if str(month).zfill(2) in data], labels=[str(month) for month in range(1, 13)])
        plt.xlabel('Month')
        plt.ylabel('Temperature (°C)')
        plt.title(f'Box Plot of Mean Temperatures from {start_year} to {end_year}')
        plt.show()

    def create_line_plot(self, year, month):
        """
        Create a line plot for daily mean temperatures of a specific month and year.
        """
        data = self.db_operations.fetch_data(start_year=year, month=month)
        # Assume data is a dict where keys are days ('01', '02', ...) and values are temperatures
        days = list(data.keys())
        temperatures = list(data.values())

        plt.plot(days, temperatures, marker='o', linestyle='-')
        plt.xlabel('Day')
        plt.ylabel('Temperature (°C)')
        plt.title(f'Daily Mean Temperatures for {month}/{year}')
        # Rotate x-axis labels for better readability
        plt.xticks(rotation=45)
        # Adjust layout to make room for the rotated x-axis labels  
        plt.tight_layout()  
        plt.show()

# Example usage
if __name__ == "__main__":
    plot_ops = PlotOperations() 
    plot_ops.create_boxplot((2000, 2020))
    plot_ops.create_line_plot(2020, 1)
