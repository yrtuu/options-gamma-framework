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

def update_forward_closes(df):
    df = df.copy()

    df["date_dt"] = pd.to_datetime(df["date"])

    for idx, row in df.iterrows():
        base_date = row["date_dt"]
        symbol = row["symbol"]

        for days, col in [(1, "close_t+1"), (2, "close_t+2"), (5, "close_t+5")]:
            target_date = (base_date + timedelta(days=days)).strftime("%Y-%m-%d")

            future_row = df[
                (df["date"] == target_date) &
                (df["symbol"] == symbol)
            ]

            if not future_row.empty:
                close_price = future_row.iloc[0]["spot"]
                df.at[idx, col] = close_price

    df.drop(columns=["date_dt"], inplace=True)
    return df



def write_back_forward_closes(df):
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

        if not mask.any():
            continue

        sheet_row = df_sheet[mask].index[0] + 2  # +2 bo header

        for col in ["close_t+1", "close_t+2", "close_t+5"]:
            if col in headers and pd.notna(row[col]) and row[col] != "":
                ws.update_cell(
                    sheet_row,
                    headers.index(col) + 1,
                    row[col]
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

def main():
    df = load_raw()

    if df.empty or "symbol" not in df.columns:
        print("No valid raw data yet — skipping postprocess")
        return

    df = update_forward_closes(df)
    write_back_forward_closes(df)

    summary = daily_summary(df)
    write_summary(summary)



if __name__ == "__main__":
    main()
