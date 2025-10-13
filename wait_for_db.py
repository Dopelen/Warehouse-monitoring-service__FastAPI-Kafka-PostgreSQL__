import os
import time
import psycopg2
from psycopg2 import OperationalError

DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_USER = os.getenv("POSTGRES_USER", "warehouse_user")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "warehouse_pass")
DB_NAME = os.getenv("POSTGRES_DB", "warehouse_db")

RETRY_INTERVAL = 2

print(f"Waiting for database {DB_HOST}:{DB_PORT}...")

while True:
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME,
        )
        conn.close()
        print("Database is available!")
        break
    except OperationalError:
        print("Database not ready, waiting...")
        time.sleep(RETRY_INTERVAL)
