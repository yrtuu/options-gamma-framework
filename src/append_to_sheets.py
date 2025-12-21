import os
import json
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

SPREADSHEET_NAME = "Options Gamma Log"
SHEET_NAME = "Sheet1"

def get_client():
    creds_json = json.loads(os.environ["GOOGLE_SHEETS_CREDENTIALS"])
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    return gspread.authorize(creds)

def append_csv(path):
    gc = get_client()
    sh = gc.open(SPREADSHEET_NAME)
    ws = sh.worksheet(SHEET_NAME)

    df = pd.read_csv(path)

    # jeśli arkusz pusty → dodaj nagłówki
    if ws.row_count == 0 or ws.get_all_values() == []:
        ws.append_row(df.columns.tolist())

    ws.append_rows(df.values.tolist(), value_input_option="USER_ENTERED")

if __name__ == "__main__":
    for file in os.listdir("data/snapshots"):
        if file.endswith(".csv"):
            append_csv(f"data/snapshots/{file}")
