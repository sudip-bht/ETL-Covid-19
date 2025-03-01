# extract.py
import os
import pandas as pd
import pycountry
import logging

logger = logging.getLogger()



def load_and_melt_data(url, value_name):
    try:
        df = pd.read_csv(url)
        melted_df = df.melt(
            id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'],
            var_name='Date',
            value_name=value_name
        )
        return melted_df
    except Exception as e:
        logger.error(f"Error loading data from {url}: {str(e)}")
        raise

def extract_data():
    logger.info("Extracting Source Data")
    try:
        urls = {
            'Confirmed': 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv',
            'Deaths': 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv',
            'Recovered': 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv'
        }
        df_confirmed = load_and_melt_data(urls['Confirmed'], 'Confirmed')
        df_deaths = load_and_melt_data(urls['Deaths'], 'Deaths')
        df_recovered = load_and_melt_data(urls['Recovered'], 'Recovered')
        df_all = df_confirmed.merge(df_deaths, on=['Province/State', 'Country/Region', 'Date', 'Lat', 'Long'], how='left')
        df_all = df_all.merge(df_recovered, on=['Province/State', 'Country/Region', 'Date', 'Lat', 'Long'], how='left')
        logger.info(f"Extracted {len(df_all)} rows from JHU CSSE datasets")
        logger.info(f"Columns: {df_all.columns.tolist()}")
        return df_all
    except Exception as e:
        logger.error(f"Error in extraction: {str(e)}")
        raise

def extract_vaccine_data():
    logger.info("Extracting Vaccine Data")
    try:
        vaccine_urls = {
            'vaccine_data': 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv',
            'vaccine_loc': 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/locations.csv'
        }
        vaccine_data = pd.read_csv(vaccine_urls['vaccine_data'])
        vaccine_loc = pd.read_csv(vaccine_urls['vaccine_loc'])
        df_vaccine = pd.merge(vaccine_data, vaccine_loc, on=["location", "iso_code"])
        logger.info(f"Extracted {len(df_vaccine)} rows of vaccine data")
        logger.info(f"Columns: {df_vaccine.columns.tolist()}")
        return df_vaccine
    except Exception as e:
        logger.error(f"Error in vaccine data extraction: {str(e)}")
        raise