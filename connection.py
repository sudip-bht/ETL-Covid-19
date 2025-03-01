# connection.py
import mysql.connector
import logging
from config import SERVER, DATABASE, USERNAME, PASSWORD

# Configure logging
logging.basicConfig(
    filename='etl_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

class DatabaseConnection:
    def __init__(self):
        self.server = SERVER  
        self.database = DATABASE
        self.username = USERNAME
        self.password = PASSWORD
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = mysql.connector.connect(
                host=self.server,
                database=self.database,
                user=self.username,
                password=self.password
            )
            self.cursor = self.conn.cursor()
            logger.info("Successfully connected to MySQL")
            return self.conn, self.cursor
        except mysql.connector.Error as e:
            logger.error(f"Error connecting to MySQL: {str(e)}")
            raise

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")