import os
import json
import gspread
import pandas as pd
from datetime import datetime
from google.oauth2.service_account import Credentials


SPREADSHEET_NAME = "Options Gamma Log"
SHEET_NAME = "raw_daily"

SCHEMA = [
    "date",
    "week",
    "symbol",
    "spot",

    "dnz_low",
    "dnz_mid",
    "dnz_high",
    "dnz_width",
    "spot_position",

    "spot_bucket",
    "gamma_bucket",
    "regime",

    "gamma_above",
    "gamma_below",
    "gamma_total",
    "gamma_diff",
    "gamma_ratio",
    "gamma_asym_strength",

    "effective_gamma_pressure",
    "egp_normalized",
    "gamma_peak_price",
    "gamma_concentration",
    "gamma_distance_from_spot",

    "close_t+1",
    "close_t+2",
    "close_t+5",

    "event_flag",
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
    df = df[SCHEMA]

    # --- HEADER ---
    if ws.get_all_values() == []:
        ws.append_row(SCHEMA)

       # --- GUARD: NIE DUPLIKUJ (date, symbol) ---
    rows = ws.get_all_values()

    if len(rows) > 1:
        header = rows[0]

        try:
            date_idx = header.index("date")
            symbol_idx = header.index("symbol")
        except ValueError:
            raise RuntimeError(
                f"Header mismatch. Found headers: {header}"
            )

        existing_keys = {
            (r[date_idx], r[symbol_idx])
            for r in rows[1:]
            if len(r) > max(date_idx, symbol_idx)
        }

        df = df[
            ~df.apply(
                lambda r: (str(r["date"]), str(r["symbol"])) in existing_keys,
                axis=1,
            )
        ]

    # jeśli po guardzie nic nie zostało → exit
    if df.empty:
        print("No new rows to append")
        return

    # --- SANITY CLEANING ---
    df = df.replace([float("inf"), float("-inf")], "")
    df = df.fillna("")

    ws.append_rows(df.values.tolist(), value_input_option="USER_ENTERED")

if __name__ == "__main__":
    today = datetime.utcnow().strftime("%Y-%m-%d")

    for file in os.listdir("data/snapshots"):
        if file.startswith(today) and file.endswith(".csv"):
            append_csv(f"data/snapshots/{file}")
