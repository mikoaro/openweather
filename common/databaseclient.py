import psycopg2
from psycopg2 import pool

class PostgresClient:
    def __init__(self, db_config):
        db_config = {
            'host': db_config.get('host'),
            'port': db_config.get('port'),
            'database': db_config.get('database'),
            'user': db_config.get('user'),
            'password': db_config.get('password')
        }
        if not all (db_config.values()):
            raise ValueError("Check database connection parameters.")
        try:
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(minconn=1, maxconn=10, **db_config)
        except Exception as e:
            raise ConnectionError(f"Error connecting to the database: {e}")
    
    def insert_data(self, data):
        conn = None
        try:
            conn = self.connection_pool.getconn()
            cursor = conn.cursor()
            insert_query = """
            INSERT INTO weather_metrics 
                (city, temperature_celsius, humidity, pressure, wind_speed, timestamp_unix)
            VALUES (%s, %s, %s, %s, %s, %s);
            """
            values = (
                data['city'],
                data['temperature_celsius'],
                data['humidity'],
                data['pressure'],
                data['wind_speed'],
                data['timestamp_unix']
            )
            cursor.execute(insert_query, values)
            conn.commit()
            cursor.close()
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error inserting data into the database: {e}")
        finally:
            if conn:
                self.connection_pool.putconn(conn)
                
    def close_pool(self):
        if self.connection_pool:
            self.connection_pool.closeall()
            print("Connection pool to PostgreSQL closed.")