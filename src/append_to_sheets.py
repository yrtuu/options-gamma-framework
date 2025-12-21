import os
import json
import gspread
import pandas as pd
from datetime import datetime
from google.oauth2.service_account import Credentials

from daily_summary import summarize_symbol

SPREADSHEET_NAME = "Options Gamma Log"
SHEET_NAME = "Sheet1"

SCHEMA = [
    "date",
    "symbol",
    "spot",
    "dnz_low",
    "dnz_mid",
    "dnz_high",
    "gamma_above",
    "gamma_below",
    "structure_tags",
]

def get_client():
    creds_json = json.loads(os.environ["GOOGLE_SHEETS_CREDENTIALS"])
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    return gspread.authorize(creds)

def append_csv(path):
    gc = get_client()
    sh = gc.open(SPREADSHEET_NAME)
    ws = sh.worksheet(SHEET_NAME)

    df = pd.read_csv(path)

    # --- STRUCTURE TAGS ---
    symbol = df.iloc[0]["symbol"]
    summary = summarize_symbol(symbol)
    if summary:
        df["structure_tags"] = summary["structure_tags"]
    else:
        df["structure_tags"] = ""

    # --- WYMUSZAMY SCHEMAT ---
    df = df[SCHEMA]

    # --- HEADER (tylko raz) ---
    if ws.get_all_values() == []:
        ws.append_row(SCHEMA)

    ws.append_rows(df.values.tolist(), value_input_option="USER_ENTERED")

if __name__ == "__main__":
    today = datetime.utcnow().strftime("%Y-%m-%d")

    for file in os.listdir("data/snapshots"):
        if file.startswith(today) and file.endswith(".csv"):
            append_csv(f"data/snapshots/{file}")
