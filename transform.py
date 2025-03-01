import pandas as pd
from datetime import datetime
import logging
import pycountry

logger = logging.getLogger()

def get_country_code(country_name):
    """Get ISO alpha-3 code for a country name."""
    try:
        return pycountry.countries.lookup(country_name).alpha_3
    except LookupError:
        return 'UNK' 

def transform_data(df):
    
    try:
        df_transformed = df.copy()

        # Standardize date format
        df_transformed['Date'] = pd.to_datetime(df_transformed['Date'], errors='coerce')

        # Ensure numeric columns, converting invalid values to 0
        df_transformed['Confirmed'] = pd.to_numeric(df_transformed['Confirmed'], errors='coerce').fillna(0).astype(int)
        df_transformed['Deaths'] = pd.to_numeric(df_transformed['Deaths'], errors='coerce').fillna(0).astype(int)
        df_transformed['Recovered'] = pd.to_numeric(df_transformed['Recovered'], errors='coerce').fillna(0).astype(int)
        df_transformed['Lat'] = pd.to_numeric(df_transformed['Lat'], errors='coerce').fillna(0)
        df_transformed['Long'] = pd.to_numeric(df_transformed['Long'], errors='coerce').fillna(0)

        # Calculate additional metrics
        df_transformed['Active'] = df_transformed['Confirmed'] - df_transformed['Deaths'] - df_transformed['Recovered']

        # Add ETL timestamp
        df_transformed['ETL_Timestamp'] = datetime.now()

        # Standardize country names and handle missing values
        df_transformed['Country/Region'] = df_transformed['Country/Region'].str.title().fillna('Unknown')
        df_transformed['Province/State'] = df_transformed['Province/State'].fillna('Unknown')

        # Add ISO country codes
        df_transformed['iso_code'] = df_transformed['Country/Region'].apply(get_country_code)

        # Rename columns to match the SQL schema in load.py
        df_transformed = df_transformed.rename(columns={
            'Province/State': 'Province_State',
            'Country/Region': 'Country_Region',
            'Long': 'Long'  # Ensure consistency if 'Long_' exists elsewhere
        })

        # Final NaN check and replacement
        df_transformed = df_transformed.fillna({
            'Province_State': 'Unknown',
            'Country_Region': 'Unknown',
            'Lat': 0,
            'Long': 0,
            'Date': pd.Timestamp('1970-01-01'),
            'Confirmed': 0,
            'Deaths': 0,
            'Recovered': 0,
            'iso_code': 'UNK',
            'Active': 0
        })

        # Log the columns for debugging
        logger.info(f"Columns after transformation: {df_transformed.columns.tolist()}")

        return df_transformed

    except Exception as e:
        logger.error(f"Error in transformation: {str(e)}")
        raise



def transform_vaccine_data(df_vaccine):
    """Transform vaccine data for loading into the database."""
    try:
        df_vaccine_transformed = df_vaccine.copy()

        # Drop unnecessary columns
        df_vaccine_transformed.drop(['daily_vaccinations_raw'], axis=1, inplace=True, errors='ignore')

        # Convert date to consistent format
        df_vaccine_transformed['date'] = pd.to_datetime(df_vaccine_transformed['date']).dt.strftime('%Y-%m-%d')

        # Rename columns
        df_vaccine_transformed = df_vaccine_transformed.rename(columns={'location': 'country'})

        # Handle OWID-specific iso_codes by removing 'OWID_' prefix
        df_vaccine_transformed['iso_code'] = df_vaccine_transformed['iso_code'].apply(
            lambda x: x.replace('OWID_', '') if pd.notnull(x) and 'OWID_' in str(x) else x
        )

        # Ensure iso_code is 3 characters long; truncate or set to 'UNK' if invalid
        df_vaccine_transformed['iso_code'] = df_vaccine_transformed['iso_code'].apply(
            lambda x: str(x)[:3] if pd.notnull(x) and len(str(x)) > 3 else (x if pd.notnull(x) else 'UNK')
        )

        # Log any problematic iso_codes for debugging
        problematic = df_vaccine_transformed[df_vaccine_transformed['iso_code'].str.len() != 3]
        if not problematic.empty:
            logger.warning(f"Found {len(problematic)} rows with invalid iso_code lengths:")
            logger.warning(problematic[['country', 'iso_code']].head().to_string())

        # Handle missing values
        for iso_code in df_vaccine_transformed['iso_code'].unique():
            mask = df_vaccine_transformed['iso_code'] == iso_code
            df_vaccine_transformed.loc[mask] = (
                df_vaccine_transformed.loc[mask]
                .ffill()  
                .fillna(0)
            )
        # Ensure numeric columns
        numeric_cols = ['total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated', 
                        'daily_vaccinations', 'total_vaccinations_per_hundred', 
                        'people_vaccinated_per_hundred', 'people_fully_vaccinated_per_hundred', 
                        'daily_vaccinations_per_million']
        for col in numeric_cols:
            if col in df_vaccine_transformed.columns:
                df_vaccine_transformed[col] = pd.to_numeric(df_vaccine_transformed[col], errors='coerce').fillna(0)

        logger.info(f"Transformed vaccine data: {len(df_vaccine)} -> {len(df_vaccine_transformed)} rows")
        logger.info(f"Vaccine data columns: {df_vaccine_transformed.columns.tolist()}")

        return df_vaccine_transformed

    except Exception as e:
        logger.error(f"Error in vaccine data transformation: {str(e)}")
        raise