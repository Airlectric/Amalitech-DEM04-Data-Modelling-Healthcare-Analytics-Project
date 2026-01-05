import os
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error

# Load credentials from .env
load_dotenv()
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_RDBMS = os.getenv("DB_RDBMS")
DB_STAR = os.getenv("DB_STAR")

# SQL scripts in the order they should run
sql_scripts = [
    ("RDBMS schema", f"schemas/rdbms_schema.sql", None),  # no DB needed to create schema
    ("Load data", f"data_generation/load_data.sql", DB_RDBMS),
    ("Star schema", f"schemas/star_schema.sql", None),  # can create star DB inside script
    ("ETL", f"etl/etl_star_schema.sql", DB_STAR)
]

def run_sql_file(file_path, database=None):
    """Execute all SQL statements in a file."""
    if not os.path.exists(file_path):
        print(f"[ERROR] SQL file not found: {file_path}")
        return False

    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=database
        )
        cursor = conn.cursor()
        print(f"[INFO] Running {file_path}...")

        with open(file_path, "r", encoding="utf-8") as f:
            sql = f.read()

        # Split statements by ';' and execute
        for statement in sql.split(";"):
            stmt = statement.strip()
            if stmt:
                try:
                    cursor.execute(stmt)
                except Error as e:
                    print(f"[ERROR] Failed on statement: {stmt[:50]}... -> {e}")
                    conn.rollback()
                    return False

        conn.commit()
        print(f"[SUCCESS] Completed {file_path}")
        cursor.close()
        conn.close()
        return True

    except Error as e:
        print(f"[ERROR] MySQL connection failed: {e}")
        return False

def main():
    for desc, file_path, db in sql_scripts:
        success = run_sql_file(file_path, db)
        if not success:
            print(f"[ABORT] Stopping execution at: {desc}")
            break

if __name__ == "__main__":
    main()
