# ETL Pipeline for COVID-19 Data

This project implements an ETL (Extract, Transform, Load) pipeline written in Python to process COVID-19 case and vaccination data. It fetches data from public sources, applies transformations, and loads the results into a MySQL database.

## Project Overview

The pipeline processes:

#### COVID-19 Case Data:

Sourced from JHU CSSE GitHub repositories (confirmed cases, deaths, recovered cases).

#### Vaccination Data:

Sourced from Our World in Data’s vaccination datasets.The transformed data is stored in two MySQL tables:

`covid_cases`: Contains case data with columns like `Province_State`, `Country_Region`, `Date`, `Confirmed`, etc.
`vaccine_data`: Contains vaccination data with columns `like country`, `iso_code`, `date`, `total_vaccinations`, etc.

## Setup Instruction

### 1. Clone the Repository

```
git clone https://github.com/your-username/etl-pipeline.git
cd etl-pipeline
```

### 2. Install Dependencies

#### Create a virtual environment

python -m venv venv

#### Activate the virtual environment

##### On Windows:

venv\Scripts\activate

##### On macOS/Linux:

source venv/bin/activate

##### Install dependencies

pip install -r requirements.txt

### 3. Set Environment Variables

Create a `.env` file  and add the following variables:

```
SERVER=localhost
DATABASE=covid_db
USERNAME=etl_user
PASSWORD=your_password
```

Replace with your mysql configuration

### 4. Executing The Pipline

```
python main.py
```

#### Check log:

The pipeline logs progress and errors to etl_pipeline.log in the project directory
Test change
