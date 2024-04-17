import os
from .dbcm import DBCM

class DBOperations:
    def __init__(self, db_name=None):
        if db_name is None:
            # Database file path.
            self.db_name = "weather_data.db"
        else:
            self.db_name = db_name
        self.initialize_db()

    def initialize_db(self):
        query = '''
                CREATE TABLE IF NOT EXISTS weather (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT,
                    location_name TEXT,
                    max_temp REAL,
                    min_temp REAL,
                    mean_temp REAL,
                    UNIQUE(date, location_name)
                );
               '''
        with DBCM(self.db_name) as cursor:
            cursor.execute(query)

    def save_data(self, date, location_name, max_temp, min_temp, mean_temp):
        query = '''
                INSERT OR IGNORE INTO weather (date, location_name, max_temp, min_temp, mean_temp)
                VALUES (?, ?, ?, ?, ?);
                '''
        with DBCM(self.db_name) as cursor:
            cursor.execute(query, (date, location_name, max_temp, min_temp, mean_temp))

    def fetch_box_data(self, start_year, end_year):
        with DBCM(self.db_name) as cursor:
            query = '''
                    SELECT strftime('%m', date) AS month, AVG(mean_temp) AS avg_temp
                    FROM weather
                    WHERE strftime('%Y', date) BETWEEN ? AND ?
                    GROUP BY strftime('%m', date)
                    ORDER BY month;
                    '''
            cursor.execute(query, (str(start_year), str(end_year)))
            result = cursor.fetchall()
            data = {row[0]: row[1] for row in result}  # Creating a dictionary from fetched data
            # Ensure every month has an entry
            for month in range(1, 13):
                month_str = str(month).zfill(2)
                if month_str not in data:
                    data[month_str] = None  
            return data
        
    def fetch_line_data(self, year, month):
        with DBCM(self.db_name) as cursor:
            query = '''
                    SELECT strftime('%d', date) AS day, mean_temp
                    FROM weather
                    WHERE strftime('%Y', date) = ? AND strftime('%m', date) = ?
                    ORDER BY date;
                    '''
            cursor.execute(query, (str(year), str(month).zfill(2)))
            result = cursor.fetchall()
            return result  # Return the list of tuples (day, mean_temp)


    def get_latest_date(self):
        query = 'SELECT MAX(date) FROM weather;'
        with DBCM(self.db_name) as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            return result[0] if result else None

    def purge_data(self):
        query = 'DELETE FROM weather;'
        with DBCM(self.db_name) as cursor:
            cursor.execute(query)

    def fetch_raw_data(self, query):
        with DBCM(self.db_name) as cursor:
            cursor.execute(query)
            return cursor.fetchall()
        
# Example usage
if __name__ == "__main__":
    db_ops = DBOperations()
    
