import os
import json
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
import snowflake.connector
from google.cloud import secretmanager
from google.oauth2 import service_account
from google.oauth2.service_account import Credentials


def get_secret(secret_id):
    client = secretmanager.SecretManagerServiceClient()
    project_id = "github-analytics-486213-u7"
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def main():
    # 1. Configuration & Auth
    spreadsheet_id = "13MDbGKTEqUWoz7hb-jHbmGBxmNMUiFhU6X6mEDMI0iM"

    # Mapping Table names to Worksheet names
    data_map = {
        "FCT_REPO_ACTIVITY_DAILY": "FCT_REPO_ACTIVITY_DAILY",
        "FCT_ISSUE_LIFECYCLE": "FCT_ISSUE_LIFECYCLE",
        "FCT_ISSUE_COMMENTS": "FCT_ISSUE_COMMENTS",
        "DIM_REPOSITORY": "DIM_REPOSITORY",
        "DIM_DATE": "DIM_DATE",
        "view_ISSUE_TIMELINE_GANTT": "view_ISSUE_TIMELINE_GANTT",
        "view_ISSUE_TIMELINE_GANTT2": "view_ISSUE_TIMELINE_GANTT2",
    }

    # Fetch Snowflake Secrets
    sf_config = json.loads(get_secret("snowflake-credentials"))

    # Google Sheets Auth
    # Assumes the Service Account JSON is stored in a Secret or the Job has the 'Sheets' role

    creds_dict = json.loads(get_secret("GOOGLE_SHEETS_SERVICE_ACCOUNT"))

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)

    sh = gc.open_by_key(spreadsheet_id)

    # 2. Snowflake Connection
    ctx = snowflake.connector.connect(
        user=sf_config["user"],
        password=sf_config["password"],
        account=sf_config["account"],
        warehouse=sf_config["warehouse"],
        database=sf_config["database"],
        schema="MARTS",
    )

    try:
        for table, sheet_name in data_map.items():
            print(f"Processing {table}...")

            # Extract
            query = f"SELECT * FROM {table}"
            df = pd.read_sql(query, ctx)

            # Target Worksheet
            worksheet = sh.worksheet(sheet_name)

            # Clear and Overwrite (keeps the GID and File ID stable for Tableau)
            worksheet.clear()
            set_with_dataframe(worksheet, df)

            print(f"Successfully updated {sheet_name}")

    finally:
        ctx.close()


if __name__ == "__main__":
    main()
