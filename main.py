# main.py
import sys
from connection import DatabaseConnection
from extract import extract_data, extract_vaccine_data
from transform import transform_data, transform_vaccine_data
from load import load_data,load_vaccine_data
import logging


logger = logging.getLogger()

def main():
    db = DatabaseConnection()
    try:
        conn, cursor = db.connect()
        logger.info("ETL process started")

        # Process COVID-19 case data
        raw_data = extract_data()
        transformed_data = transform_data(raw_data)
        load_data(cursor, conn, transformed_data,)

        # Process vaccine data
        raw_vaccine_data = extract_vaccine_data()
        transformed_vaccine_data = transform_vaccine_data(raw_vaccine_data)
        load_vaccine_data(cursor, conn, transformed_vaccine_data)

        logger.info("ETL process completed")
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}")
        sys.exit(1)
    finally:
        db.close()

if __name__ == "__main__":
    main()