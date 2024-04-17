import sqlite3

class DBCM:
    def __init__(self, db_name):
        """
        Initialize the database connection manager with the name of the database.
        :param db_name: The name of the database file.
        """
        self.db_name = db_name
        self.conn = None
        self.cursor = None

    def __enter__(self):
        """
        Enter the runtime context related to this object.
        The with statement will bind this methods return value to the target(s) specified in the as clause of the statement.
        Here, it will return a cursor object from the database connection.
        """
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the runtime context and close the database connection.
        If there was any exception, it rolls back any transaction that might have happened. Otherwise, it commits the transactions.
        :param exc_type: Exception type.
        :param exc_val: Exception value.
        :param exc_tb: Exception traceback.
        """
        if exc_type or exc_val or exc_tb:
            self.conn.rollback()
        else:
            self.conn.commit()

        self.cursor.close()
        self.conn.close()



# Example usage
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
