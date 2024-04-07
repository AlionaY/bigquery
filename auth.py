import os

from google.cloud import bigquery
import gspread
from oauth2client.service_account import ServiceAccountCredentials

import config


def authenticate_google_sheets():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.KEY_PATH
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(config.CREDENTIALS_FILE, scope)
    google_client = gspread.authorize(credentials)
    sheet = google_client.open(config.SHEET_NAME)
    return sheet


def authenticate_bigquery():
    return bigquery.Client(project=config.PROJECT_ID)
