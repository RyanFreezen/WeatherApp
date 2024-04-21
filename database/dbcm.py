import sqlite3

class DBCM:
    def __init__(self, db_name):
        """
        Initialize the database connection manager.
        """
        self.db_name = db_name
        self.conn = None
        self.cursor = None

    def __enter__(self):
        """
        Enter the runtime context related to this object.
        It will return a cursor object from the database connection.
        """
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the runtime context and close the database connection.
        If there was any exception, it rolls back any transaction that might have happened. 
        Otherwise, it commits the transactions.
        """
        if exc_type or exc_val or exc_tb:
            self.conn.rollback()
        else:
            self.conn.commit()

        self.cursor.close()
        self.conn.close()

if __name__ == "__main__":
    db_name = "weather_data.db"
    with DBCM(db_name) as cursor:
        cursor.execute('''CREATE TABLE IF NOT EXISTS weather (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            date TEXT,
                            location_name TEXT,
                            max_temp REAL,
                            min_temp REAL,
                            mean_temp REAL,
                            UNIQUE(date, location_name)
                          );''')
        print("Table created or already exists.")