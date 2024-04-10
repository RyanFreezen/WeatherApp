import sqlite3

class DBOperations:
    def __init__(self, db_name="weather_data.db"):
        self.db_name = db_name
        self.initialize_db()

    def _connect(self):
        """
        Private method to handle database connections.
        This creates a connection to the SQLite database specified by db_name.
        """
        return sqlite3.connect(self.db_name)

    def initialize_db(self):
        """
        Initializes the database and creates the weather table if it does not exist.
        This table stores weather data, including the sample date, location, and temperature readings.
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
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query)

    def save_data(self, date, location_name, max_temp, min_temp, mean_temp):
        """
        Inserts or ignores a new record into the weather table.
        This method avoids duplication by using the UNIQUE constraint on date and location_name.
        """
        query = '''
                INSERT OR IGNORE INTO weather (date, location_name, max_temp, min_temp, mean_temp)
                VALUES (?, ?, ?, ?, ?);
                '''
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (date, location_name, max_temp, min_temp, mean_temp))
            conn.commit()

    def fetch_data(self):
        """
        Fetches all records from the weather table.
        This method is useful for retrieving stored weather data for analysis or reporting.
        """
        query = 'SELECT * FROM weather;'
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            return cursor.fetchall()

    def purge_data(self):
        """
        Deletes all records from the weather table.
        This method is used to clear the database of existing data without removing the table structure.
        """
        query = 'DELETE FROM weather;'
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()

# Example usage
if __name__ == "__main__":
    db_ops = DBOperations()
    
    # Example of inserting sample data
    """
    db_ops.save_data('2024-04-08', 'Winnipeg', -3, 5, 1)
    db_ops.save_data('2024-04-09', 'Winnipeg', 2, 7, 4.5)
    """

    
    # Fetching and printing all records
    records = db_ops.fetch_data()
    print("Weather Data:")
    for record in records:
        print(record)
    
    # Optional: Uncomment to purge data from the database
    # db_ops.purge_data()
