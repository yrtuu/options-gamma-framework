import os
import json
import gspread
import pandas as pd
from datetime import datetime
from google.oauth2.service_account import Credentials

SPREADSHEET_NAME = "Options Gamma Log"
SHEET_NAME = "raw_daily"

# ⬇️ tylko KOLUMNY PRODUKOWANE PRZEZ main.py
BASE_SCHEMA = [
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

    # 🔒 upewnij się, że CSV ma wymagane kolumny
    missing = [c for c in BASE_SCHEMA if c not in df.columns]
    if missing:
        raise RuntimeError(f"CSV missing columns: {missing}")

    df = df[BASE_SCHEMA]

    values = ws.get_all_values()

    # --- INIT SHEET ---
    if not values:
        ws.append_row(BASE_SCHEMA)
        sheet_header = BASE_SCHEMA
        existing_keys = set()
    else:
        sheet_header = [h.strip() for h in values[0]]

        # 🔴 KLUCZOWA ZMIANA: NIE WYMAGAMY RÓWNOŚCI
        for col in BASE_SCHEMA:
            if col not in sheet_header:
                raise RuntimeError(f"Sheet missing required column: {col}")

        date_idx = sheet_header.index("date")
        symbol_idx = sheet_header.index("symbol")

        existing_keys = {
            (r[date_idx], r[symbol_idx])
            for r in values[1:]
            if len(r) > max(date_idx, symbol_idx)
        }

    # --- DUPLICATE GUARD ---
    rows_to_add = []
    for _, r in df.iterrows():
        key = (str(r["date"]), str(r["symbol"]))
        if key not in existing_keys:
            # ⬇️ wypełniamy tylko kolumny BASE_SCHEMA
            row = [r.get(col, "") for col in sheet_header]
            rows_to_add.append(row)

    if not rows_to_add:
        print(f"[SKIP] {path} — no new rows")
        return

    ws.append_rows(rows_to_add, value_input_option="USER_ENTERED")
    print(f"[OK] Appended {len(rows_to_add)} rows from {path}")

if __name__ == "__main__":
    today = datetime.utcnow().strftime("%Y-%m-%d")
    for file in os.listdir("data/snapshots"):
        if file.startswith(today) and file.endswith(".csv"):
            append_csv(f"data/snapshots/{file}")