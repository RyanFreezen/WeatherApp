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
        """
        Function to initialize database.
        """
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
        """
        Function to save data to the database.
        """
        query = '''
                INSERT OR IGNORE INTO weather (date, location_name, max_temp, min_temp, mean_temp)
                VALUES (?, ?, ?, ?, ?);
                '''
        with DBCM(self.db_name) as cursor:
            cursor.execute(query, (date, location_name, max_temp, min_temp, mean_temp))

    def fetch_box_data(self, start_year, end_year):
        """
        Function to fetch all data for box plot operations.
        """
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
            # Creating a dictionary from fetched data
            data = {row[0]: row[1] for row in result}
            # Ensure every month has an entry
            for month in range(1, 13):
                month_str = str(month).zfill(2)
                if month_str not in data:
                    data[month_str] = None
            return data

    def fetch_line_data(self, year, month):
        """
        Function to fetch all data for line plot operations.
        """
        with DBCM(self.db_name) as cursor:
            query = '''
                    SELECT strftime('%d', date) AS day, mean_temp
                    FROM weather
                    WHERE strftime('%Y', date) = ? AND strftime('%m', date) = ?
                    ORDER BY date;
                    '''
            cursor.execute(query, (str(year), str(month).zfill(2)))
            result = cursor.fetchall()
            return result


    def get_latest_date(self):
        """
        Function to retrieve the latest date from weather table.
        """
        query = 'SELECT MAX(date) FROM weather;'
        with DBCM(self.db_name) as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            return result[0] if result else None

    def purge_data(self):
        """
        Function to delete all data from weather table.
        """
        query = 'DELETE FROM weather;'
        with DBCM(self.db_name) as cursor:
            cursor.execute(query)

    def fetch_raw_data(self, query):
        """
        Function to fetch all data..
        """
        with DBCM(self.db_name) as cursor:
            cursor.execute(query)
            return cursor.fetchall()

if __name__ == "__main__":
    db_ops = DBOperations()