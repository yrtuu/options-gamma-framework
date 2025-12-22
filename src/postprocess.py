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


# 🔴 KLUCZOWA POPRAWKA – ODPORNE WCZYTYWANIE SHEETS
def load_raw():
    gc = get_client()
    sh = gc.open(SPREADSHEET_NAME)
    ws = sh.worksheet(RAW_SHEET)

    values = ws.get_all_values()

    if len(values) < 2:
        return pd.DataFrame()

    headers = [h.strip().lower() for h in values[0]]
    rows = values[1:]

    df = pd.DataFrame(rows, columns=headers)

    return df


def add_days_to_close(df):
    for n in [1, 2, 5]:
        df[f"days_to_close_t+{n}"] = n
    return df


def update_forward_closes(df):
    df = df.copy()

    df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date_dt"])
    df = df.sort_values(["symbol", "date_dt"])

    for symbol, g in df.groupby("symbol"):
        g = g.reset_index()

        for i, row in g.iterrows():
            base_idx = row["index"]

            for n, col in [(1, "close_t+1"), (2, "close_t+2"), (5, "close_t+5")]:
                if i + n < len(g):
                    df.at[base_idx, col] = g.loc[i + n, "spot"]

    df.drop(columns=["date_dt"], inplace=True)
    return df


def compute_forward_returns(df):
    df = df.copy()

    for n in [1, 2, 5]:
        close_col = f"close_t+{n}"
        ret_col = f"ret_t+{n}"

        df[ret_col] = ""

        mask = df[close_col].notna() & (df[close_col] != "")
        df.loc[mask, ret_col] = (
            df.loc[mask, close_col].astype(float)
            / df.loc[mask, "spot"].astype(float)
            - 1
        )

    return df


def write_back_forward_closes(df):
    gc = get_client()
    sh = gc.open(SPREADSHEET_NAME)
    ws = sh.worksheet(RAW_SHEET)

    records = ws.get_all_records()
    headers = ws.row_values(1)
    df_sheet = pd.DataFrame(records)

    for _, row in df.iterrows():
        mask = (
            (df_sheet["date"] == row["date"])
            & (df_sheet["symbol"] == row["symbol"])
        )

        if not mask.any():
            continue

        sheet_row = df_sheet[mask].index[0] + 2

        for col in [
            "close_t+1", "close_t+2", "close_t+5",
            "ret_t+1", "ret_t+2", "ret_t+5",
            "days_to_close_t+1", "days_to_close_t+2", "days_to_close_t+5",
        ]:
            if col in headers and pd.notna(row[col]) and row[col] != "":
                ws.update_cell(
                    sheet_row,
                    headers.index(col) + 1,
                    row[col]
                )


def daily_summary(df):
    today = datetime.utcnow().strftime("%Y-%m-%d")
    today_df = df[df["date"] == today]

    if today_df.empty:
        return None

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
    if not summary:
        return

    gc = get_client()
    sh = gc.open(SPREADSHEET_NAME)
    ws = sh.worksheet(SUMMARY_SHEET)

    if ws.get_all_values() == []:
        ws.append_row(list(summary.keys()))

    ws.append_row(list(summary.values()))


def main():
    df = load_raw()

    print("RAW_DAILY columns:", df.columns.tolist())

    if df.empty or "symbol" not in df.columns or "date" not in df.columns:
        print("No valid raw data yet — skipping postprocess")
        return

    df = update_forward_closes(df)
    df = compute_forward_returns(df)
    df = add_days_to_close(df)
    write_back_forward_closes(df)

    summary = daily_summary(df)
    write_summary(summary)


if __name__ == "__main__":
    main()
