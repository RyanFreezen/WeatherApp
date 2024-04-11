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

    def fetch_data(self, start_year=None, end_year=None, month=None):
        """
        Fetches weather data from the database. Flexible to fetch either:
        - Aggregated monthly data over a range of years
        - Daily data for a specific month and year
        """
        if month and start_year and end_year is None:
            # Fetch daily data for a specific month and year
            query = f'''
                    SELECT strftime('%d', date) AS day, mean_temp
                    FROM weather
                    WHERE strftime('%Y', date) = ? AND strftime('%m', date) = '{str(month).zfill(2)}'
                    ORDER BY day;
                    '''
            with DBCM(self.db_name) as cursor:
                cursor.execute(query, (start_year,))
                return {row[0]: row[1] for row in cursor.fetchall()}

        else:
            # Fetch aggregated monthly data over a range of years
            query = f'''
                    SELECT strftime('%m', date) AS month, AVG(mean_temp) AS avg_temp
                    FROM weather
                    WHERE strftime('%Y', date) BETWEEN ? AND ?
                    GROUP BY strftime('%m', date)
                    ORDER BY month;
                    '''
            with DBCM(self.db_name) as cursor:
                cursor.execute(query, (start_year, end_year))
                return {row[0]: row[1] for row in cursor.fetchall()}
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

# Example usage
if __name__ == "__main__":
    db_ops = DBOperations()
    
