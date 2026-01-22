import mysql.connector
from mysql.connector import Error

# Config (update with your creds)
DB_CONFIG = {
    'host': 'localhost',
    'user': 'your_username',
    'password': 'your_password',
    'database': 'hospital_star_db'
}

SQL_FILE = './incremental_etl.sql'  

def execute_etl():
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Read and execute SQL file
        with open(SQL_FILE, 'r') as f:
            sql_script = f.read()
            for statement in sql_script.split(';'):
                if statement.strip():
                    cursor.execute(statement)
        
        conn.commit()
        print("ETL completed successfully.")
    except Error as e:
        if conn:
            conn.rollback()
            # Update status to FAILURE (add error_message param if needed)
            cursor.execute("""
                INSERT INTO etl_control (load_type, load_date, records_processed, status, error_message)
                VALUES ('INCREMENTAL', NOW(), 0, 'FAILURE', %s)
            """, (str(e),))
            conn.commit()
        print(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    execute_etl()