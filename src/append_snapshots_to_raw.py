import os
import json
import math
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from pathlib import Path


# ================= CONFIG =================
SPREADSHEET_NAME = "Options Gamma Log"
RAW_SHEET = "raw_daily"
DATA_PATH = Path("data/snapshots")

REQUIRED_COLUMNS = {"date", "symbol"}

EXPECTED_HEADER = [
    "date","week","symbol","spot",
    "dnz_low","dnz_mid","dnz_high","dnz_width",
    "spot_position","spot_bucket","gamma_bucket","regime",
    "gamma_above","gamma_below","gamma_total","gamma_diff","gamma_ratio",
    "gamma_asym_strength","effective_gamma_pressure","egp_normalized",
    "gamma_peak_price","gamma_concentration","gamma_distance_from_spot",
    "close_t+1","close_t+2","close_t+5",
    "ret_t+1","ret_t+2","ret_t+5",
    "days_to_close_t+1","days_to_close_t+2","days_to_close_t+5",
    "data_ok","event_flag",
    "is_event_day","event_type","event_phase",
]

if header != EXPECTED_HEADER:
    raise RuntimeError(
        "❌ RAW_DAILY SCHEMA DRIFT — STOP PIPELINE\n"
        f"Expected: {EXPECTED_HEADER}\n"
        f"Found:    {header}"
    )



# ================= AUTH =================
def get_client():
    creds_json = json.loads(os.environ["GOOGLE_SHEETS_CREDENTIALS"])
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    return gspread.authorize(creds)


# ================= LOAD EXISTING RAW =================
def load_existing_keys_and_header(ws):
    values = ws.get_all_values()

    if not values:
        raise RuntimeError("❌ RAW sheet is empty — header required")

    header = [h.strip().lower() for h in values[0]]

    # 🔒 HARD SCHEMA GUARD
    if header != EXPECTED_HEADER:
        raise RuntimeError(
            "❌ RAW_DAILY SCHEMA DRIFT — STOP APPEND\n"
            f"Expected: {EXPECTED_HEADER}\n"
            f"Found:    {header}"
        )

    date_idx = header.index("date")
    symbol_idx = header.index("symbol")

    keys = set()
    for r in values[1:]:
        if len(r) > max(date_idx, symbol_idx):
            keys.add((r[date_idx], r[symbol_idx]))

    # next free row (1-indexed)
    start_row = len(values) + 1

    return keys, header, start_row


# ================= CLEAN CELL =================
def clean_value(x):
    if x is None:
        return ""
    if isinstance(x, float):
        if math.isnan(x) or math.isinf(x):
            return ""
        return float(x)
    return x


# ================= SAFE APPEND =================
def append_rows_strict(ws, rows, header, start_row):
    col_map = {col: i + 1 for i, col in enumerate(header)}
    updates = []

    for i, row in enumerate(rows):
        sheet_row = start_row + i
        for col, val in zip(header, row):
            if val in ["", None]:
                continue
            updates.append({
                "range": gspread.utils.rowcol_to_a1(sheet_row, col_map[col]),
                "values": [[val]],
            })

    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")
        print(f"[OK] Appended {len(rows)} new raw rows (schema-safe)")
    else:
        print("[OK] Nothing to append")


# ================= MAIN =================
def main():
    if not DATA_PATH.exists():
        print("[EXIT] No snapshots directory")
        return

    gc = get_client()
    ws = gc.open(SPREADSHEET_NAME).worksheet(RAW_SHEET)

    existing_keys, header, start_row = load_existing_keys_and_header(ws)
    rows_to_add = []

    for file in sorted(DATA_PATH.glob("*.csv")):
        try:
            df = pd.read_csv(file)
        except Exception as e:
            print(f"[SKIP] {file.name} — cannot read CSV ({e})")
            continue

        df.columns = [c.strip().lower() for c in df.columns]

        # 🔒 snapshot sanity check
        if not REQUIRED_COLUMNS.issubset(df.columns):
            print(
                f"[SKIP] {file.name} — missing columns "
                f"{REQUIRED_COLUMNS - set(df.columns)}"
            )
            continue

        for _, r in df.iterrows():
            key = (str(r["date"]), str(r["symbol"]))
            if key in existing_keys:
                continue

            row = []
            for col in header:
                val = r[col] if col in r else ""
                row.append(clean_value(val))

            rows_to_add.append(row)
            existing_keys.add(key)

    if not rows_to_add:
        print("[OK] No new snapshot rows to append")
        return

    append_rows_strict(ws, rows_to_add, header, start_row)


if __name__ == "__main__":
    main()
