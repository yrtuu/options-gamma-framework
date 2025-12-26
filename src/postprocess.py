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

    return df. 

def write_back_forward_closes(df):
    gc = get_client()
    sh = gc.open(SPREADSHEET_NAME)
    ws = sh.worksheet(RAW_SHEET)

    records = ws.get_all_records()
    df_sheet = pd.DataFrame(records)
    df_sheet.columns = [c.strip().lower() for c in df_sheet.columns]

    headers = ws.row_values(1)
    headers_l = [h.lower() for h in headers]

    update_rows = []

    for _, row in df.iterrows():
        mask = (
            (df_sheet["date"] == row["date"]) &
            (df_sheet["symbol"] == row["symbol"])
        )

        if not mask.any():
            continue

        sheet_row = df_sheet[mask].index[0] + 2

        updated = ws.row_values(sheet_row)
        updated += [""] * (len(headers) - len(updated))

        for col in [
            "close_t+1", "close_t+2", "close_t+5",
            "ret_t+1", "ret_t+2", "ret_t+5",
            "days_to_close_t+1", "days_to_close_t+2", "days_to_close_t+5",
        ]:
            if col in headers_l:
                idx = headers_l.index(col)
                val = row[col]
                if val != "" and pd.notna(val):
                    updated[idx] = val

        update_rows.append((sheet_row, updated))

    # 🔴 JEDEN REQUEST NA WIERSZ (a nie na komórkę)
    for r, values in update_rows:
        ws.update(f"A{r}", [values], value_input_option="USER_ENTERED")   
 

def write_summary(summary):
    gc = get_client()
    sh = gc.open(SPREADSHEET_NAME)
    ws = sh.worksheet(SUMMARY_SHEET)

    # --- FORCE PYTHON NATIVE TYPES (CRITICAL FIX) ---
    clean_summary = {}
    for k, v in summary.items():
        if isinstance(v, (pd.Int64Dtype,)):
            clean_summary[k] = int(v)
        elif hasattr(v, "item"):  # numpy scalar
            clean_summary[k] = v.item()
        else:
            clean_summary[k] = v

    if ws.get_all_values() == []:
        ws.append_row(list(clean_summary.keys()), value_input_option="RAW")

    ws.append_row(list(clean_summary.values()), value_input_option="RAW")


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
