import os
import json
import math
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

    "ret_t+1",
    "ret_t+2",
    "ret_t+5",

    "days_to_close_t+1",
    "days_to_close_t+2",
    "days_to_close_t+5",

    "event_flag",
]

# ---------- AUTH ----------
def get_client():
    creds_json = json.loads(os.environ["GOOGLE_SHEETS_CREDENTIALS"])
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    return gspread.authorize(creds)

# ---------- CORE ----------
def append_csv(path):
    gc = get_client()
    sh = gc.open(SPREADSHEET_NAME)
    ws = sh.worksheet(SHEET_NAME)

    # ---- LOAD CSV ----
    df = pd.read_csv(path)

    # zapewnij wszystkie kolumny schematu
    for col in SCHEMA:
        if col not in df.columns:
            df[col] = ""

    df = df[SCHEMA]

    # ---- READ SHEET ----
    values = ws.get_all_values()

    # CASE 1: arkusz pusty
    if len(values) == 0:
        ws.append_row(SCHEMA, value_input_option="RAW")
        existing_keys = set()

    # CASE 2: arkusz ma tylko pusty wiersz
    elif len(values) == 1 and all(v == "" for v in values[0]):
        ws.update("A1", [SCHEMA])
        existing_keys = set()

    # CASE 3: normalny arkusz
    else:
        header = [h.strip() for h in values[0]]
        if header != SCHEMA:
            raise RuntimeError(
                f"Header mismatch.\nExpected: {SCHEMA}\nFound: {header}"
            )

        date_idx = header.index("date")
        symbol_idx = header.index("symbol")

        existing_keys = {
            (r[date_idx], r[symbol_idx])
            for r in values[1:]
            if len(r) > max(date_idx, symbol_idx)
        }

    # ---- BUILD ROWS (ANTI-NaN / ANTI-INF) ----
    rows_to_add = []

    for _, r in df.iterrows():
        key = (str(r["date"]), str(r["symbol"]))
        if key in existing_keys:
            continue

        clean_row = []
        for x in r.tolist():
            if x is None:
                clean_row.append("")
            elif isinstance(x, float):
                if math.isnan(x) or math.isinf(x):
                    clean_row.append("")
                else:
                    clean_row.append(float(x))
            else:
                clean_row.append(x)

        rows_to_add.append(clean_row)

    if not rows_to_add:
        print(f"[SKIP] {path} — no new rows")
        return

    # ---- APPEND ----
    ws.append_rows(
        rows_to_add,
        value_input_option="USER_ENTERED",
    )

    print(f"[OK] Appended {len(rows_to_add)} rows from {path}")

# ---------- ENTRY ----------
if __name__ == "__main__":
    today = datetime.utcnow().strftime("%Y-%m-%d")

    for file in os.listdir("data/snapshots"):
        if file.startswith(today) and file.endswith(".csv"):
            append_csv(f"data/snapshots/{file}")