from dotenv import load_dotenv
import os
import pymysql

load_dotenv()  # This loads the environment variables from .env

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

# Load environment variables
load_dotenv()


def create_db_connection():
    try:
        connection = pymysql.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            db=os.getenv('DB_NAME'),
            port=int(os.getenv('DB_PORT'))
        )
        print("Database connection successfully established.")
        return connection
    except pymysql.MySQLError as e:
        print(f"Error connecting to the MariaDB Database: {e}")
        return None


# Example usage
db_connection = create_db_connection()
if db_connection is not None:
    # Proceed with operations using db_connection
    db_connection.close()
