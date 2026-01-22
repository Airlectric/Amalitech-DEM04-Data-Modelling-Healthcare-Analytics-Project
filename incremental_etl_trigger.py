import os
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_STAR")
}

SQL_FILE = "incremental_etl.sql"


def execute_etl():
    conn = None
    cursor = None

    try:
        # Create DB connection
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Read SQL script
        with open(SQL_FILE, "r") as file:
            sql_script = file.read()

        # Execute each statement
        for statement in sql_script.split(";"):
            stmt = statement.strip()
            if stmt:
                cursor.execute(stmt)

        # Commit transaction
        conn.commit()

        # Log success
        cursor.execute("""
            INSERT INTO etl_control (
                load_type,
                load_date,
                records_processed,
                status,
                error_message
            )
            VALUES ('INCREMENTAL', NOW(), NULL, 'SUCCESS', NULL)
        """)
        conn.commit()

        print("ETL completed successfully.")

    except Error as e:
        if conn:
            conn.rollback()

        if conn and cursor:
            cursor.execute("""
                INSERT INTO etl_control (
                    load_type,
                    load_date,
                    records_processed,
                    status,
                    error_message
                )
                VALUES ('INCREMENTAL', NOW(), 0, 'FAILURE', %s)
            """, (str(e),))
            conn.commit()

        print(f"ETL failed: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    execute_etl()
