# test_connection.py
import mysql.connector
from config import SERVER, DATABASE, USERNAME, PASSWORD

try:
    conn = mysql.connector.connect(
        host=SERVER,
        database=DATABASE,
        user=USERNAME,
        password=PASSWORD
    )
    print("Connection successful!")
    conn.close()
except mysql.connector.Error as e:
    print(f"Connection failed: {e}")