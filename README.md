**Overview**

This Python script collects data from BigQuery, performs simple aggregations on the data, and writes the resulting dataframes to a Google Sheet.
**Spent time for writing dataframes to the google sheet: 8.7 sec**

**Links**

Google sheet with the resulting dataframes: https://docs.google.com/spreadsheets/d/1I-soTYjydH0Crr8ivoT7haPIdJq44mECzkzTzO_goSA/edit#gid=0

**Prerequisites**
- Python 3.x installed on your system
- bigquery library installed (install using pip install google-cloud-bigquery)
- pandas library installed (install using pip install pandas)
- gspread library installed (install using pip install gspread)
- A Google Cloud Platform project with BigQuery API enabled and appropriate credentials set up
- Google Sheets API enabled for your project and appropriate credentials set up
- concurrent.futures library installed (pip install futures)

**Files**
- main.py: The main Python script that orchestrates the data collection, aggregation, and writing process.
- config.py: Configuration file containing project and dataset details.
- query.sql: SQL query file for fetching data from BigQuery.
- auth.py: Contains authentication functions for accessing Google services.
