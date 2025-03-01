# load.py
import logging
from config import TARGET_TABLE,VACCINE_TABLE


logger = logging.getLogger()

def load_data(cursor, conn, df):
    logger.info(f"DataFrame columns before loading: {df.columns.tolist()}")
    try:
        if df.isnull().values.any():
            logger.error("NaN values still present after transformation!")
            raise ValueError("DataFrame contains NaN values")
        # Create table with quoted column names
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            `id` INT AUTO_INCREMENT PRIMARY KEY,
            `Province_State` VARCHAR(100),
            `Country_Region` VARCHAR(100),
            `Lat` FLOAT,
            `Long` FLOAT,
            `Date` DATETIME,
            `Confirmed` INT,
            `Deaths` INT,
            `Recovered` INT,
            `iso_code` VARCHAR(3),
            `Active` INT
        )
        """
        cursor.execute(create_table_query)

        # Truncate table (optional - remove if you want to append data)
        cursor.execute(f"TRUNCATE TABLE {TARGET_TABLE}")

        # Insert data with quoted column names
        insert_query = f"""
        INSERT INTO {TARGET_TABLE} 
        (`Province_State`, `Country_Region`, `Lat`, `Long`, `Date`, `Confirmed`, `Deaths`, `Recovered`, `iso_code`, `Active`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Convert DataFrame to list of tuples using standardized column names
        values = [tuple(row) for row in df[
            ['Province_State', 'Country_Region', 'Lat', 'Long', 'Date', 
             'Confirmed', 'Deaths', 'Recovered', 'iso_code', 'Active']
        ].values]

        cursor.executemany(insert_query, values)
        conn.commit()

        logger.info(f"Loaded {len(df)} rows into {TARGET_TABLE}")
    except Exception as e:
        logger.error(f"Error in loading: {str(e)}")
        conn.rollback()
        raise




def load_vaccine_data(cursor, conn, df):
    logger.info(f"DataFrame columns before loading: {df.columns.tolist()}")
    # Check iso_code lengths
    problematic_rows = df[df['iso_code'].str.len() > 3]
    if not problematic_rows.empty:
        logger.error(f"Found {len(problematic_rows)} rows with iso_code longer than 3 characters:")
        logger.error(problematic_rows[['country', 'iso_code', 'date']].to_string())
    try:
        # Check for NaN values
        if df.isnull().values.any():
            logger.error("NaN values still present after transformation!")
            raise ValueError("DataFrame contains NaN values")

        # Create table with quoted column names for vaccine data
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {VACCINE_TABLE} (
            `id` INT AUTO_INCREMENT PRIMARY KEY,
            `country` VARCHAR(100),
            `iso_code` VARCHAR(3),
            `date` DATETIME,
            `total_vaccinations` FLOAT,
            `people_vaccinated` FLOAT,
            `people_fully_vaccinated` FLOAT,
            `daily_vaccinations` FLOAT,
            `total_vaccinations_per_hundred` FLOAT,
            `people_vaccinated_per_hundred` FLOAT,
            `people_fully_vaccinated_per_hundred` FLOAT,
            `daily_vaccinations_per_million` FLOAT
        )
        """
        cursor.execute(create_table_query)

        # Truncate table (optional - remove if you want to append data)
        cursor.execute(f"TRUNCATE TABLE {VACCINE_TABLE}")

        # Insert data with quoted column names
        insert_query = f"""
        INSERT INTO {VACCINE_TABLE} 
        (`country`, `iso_code`, `date`, `total_vaccinations`, `people_vaccinated`, 
         `people_fully_vaccinated`, `daily_vaccinations`, `total_vaccinations_per_hundred`, 
         `people_vaccinated_per_hundred`, `people_fully_vaccinated_per_hundred`, 
         `daily_vaccinations_per_million`)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Convert DataFrame to list of tuples using standardized column names
        values = [tuple(row) for row in df[
            ['country', 'iso_code', 'date', 'total_vaccinations', 'people_vaccinated', 
             'people_fully_vaccinated', 'daily_vaccinations', 'total_vaccinations_per_hundred', 
             'people_vaccinated_per_hundred', 'people_fully_vaccinated_per_hundred', 
             'daily_vaccinations_per_million']
        ].values]

        cursor.executemany(insert_query, values)
        conn.commit()

        logger.info(f"Loaded {len(df)} rows into {VACCINE_TABLE}")

    except Exception as e:
        logger.error(f"Error in loading vaccine data: {str(e)}")
        conn.rollback()
        raise