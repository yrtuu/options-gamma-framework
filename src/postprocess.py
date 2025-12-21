import os
import json
import pandas as pd
import gspread
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

SPREADSHEET_NAME = "Options Gamma Log"
RAW_SHEET = "raw_daily"
SUMMARY_SHEET = "daily_summary"

def get_client():
    creds_json = json.loads(os.environ["GOOGLE_SHEETS_CREDENTIALS"])
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    return gspread.authorize(creds)

def load_raw():
    gc = get_client()
    sh = gc.open(SPREADSHEET_NAME)
    ws = sh.worksheet(RAW_SHEET)
    data = ws.get_all_records()
    return pd.DataFrame(data)

def update_close_t1(df):
    today = datetime.utcnow().date()
    yesterday = today - timedelta(days=1)

    for symbol in df["symbol"].unique():
        today_row = df[(df["date"] == str(today)) & (df["symbol"] == symbol)]
        y_row = df[(df["date"] == str(yesterday)) & (df["symbol"] == symbol)]

        if not today_row.empty and not y_row.empty:
            close = today_row.iloc[0]["spot"]
            df.loc[y_row.index, "close_t+1"] = close

    return df


def write_back_close_t1(df):
    gc = get_client()
    sh = gc.open(SPREADSHEET_NAME)
    ws = sh.worksheet(RAW_SHEET)

    records = ws.get_all_records()
    headers = ws.row_values(1)

    df_sheet = pd.DataFrame(records)

    for idx, row in df.iterrows():
        mask = (
            (df_sheet["date"] == row["date"]) &
            (df_sheet["symbol"] == row["symbol"])
        )
        if mask.any():
            sheet_row = df_sheet[mask].index[0] + 2  # +2 bo header
            ws.update_cell(
                sheet_row,
                headers.index("close_t+1") + 1,
                row["close_t+1"]
            )


def daily_summary(df):
    today = datetime.utcnow().strftime("%Y-%m-%d")
    today_df = df[df["date"] == today]

    counts = today_df["regime"].value_counts()
    dominant = counts.idxmax()
    share = counts.max() / counts.sum()

    return {
        "date": today,
        "dominant_regime": dominant,
        "share": round(share, 2),
        "symbols": counts.sum(),
    }


def write_summary(summary):
    gc = get_client()
    sh = gc.open(SPREADSHEET_NAME)
    ws = sh.worksheet(SUMMARY_SHEET)

    if ws.get_all_values() == []:
        ws.append_row(list(summary.keys()))

    ws.append_row(list(summary.values()))
