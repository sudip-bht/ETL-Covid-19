from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Configuration from environment variables
SERVER = os.getenv('SERVER')
DATABASE = os.getenv('DATABASE')
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')
TARGET_TABLE = 'covid_cases'
VACCINE_TABLE = 'vaccine_data'
# Validate configuration
required_vars = ['SERVER', 'DATABASE', 'USERNAME', 'PASSWORD', 'TARGET_TABLE']
for var in required_vars:
    if not globals()[var]:
        raise ValueError(f"Missing required environment variable: {var}")