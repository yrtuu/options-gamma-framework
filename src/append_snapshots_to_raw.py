import os
import json
import math
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from pathlib import Path

SPREADSHEET_NAME = "Options Gamma Log"
RAW_SHEET = "raw_daily"
DATA_PATH = Path("data/snapshots")

REQUIRED_COLUMNS = {"date", "symbol"}


# ================= AUTH =================
def get_client():
    creds_json = json.loads(os.environ["GOOGLE_SHEETS_CREDENTIALS"])
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    return gspread.authorize(creds)


# ================= LOAD RAW KEYS =================
def load_existing_keys(ws):
    values = ws.get_all_values()
    if len(values) < 2:
        return set(), values[0] if values else []

    header = [h.strip().lower() for h in values[0]]

    if "date" not in header or "symbol" not in header:
        raise RuntimeError("RAW sheet must contain 'date' and 'symbol' columns")

    date_idx = header.index("date")
    symbol_idx = header.index("symbol")

    keys = {
        (r[date_idx], r[symbol_idx])
        for r in values[1:]
        if len(r) > max(date_idx, symbol_idx)
    }

    return keys, header


# ================= CLEAN CELL =================
def clean_value(x):
    if x is None:
        return ""
    if isinstance(x, float):
        if math.isnan(x) or math.isinf(x):
            return ""
        return float(x)
    return x


# ================= MAIN =================
def main():
    if not DATA_PATH.exists():
        print("[EXIT] No snapshots directory")
        return

    gc = get_client()
    sh = gc.open(SPREADSHEET_NAME)
    ws = sh.worksheet(RAW_SHEET)

    existing_keys, header = load_existing_keys(ws)
    rows_to_add = []

    for file in sorted(DATA_PATH.glob("*.csv")):
        try:
            df = pd.read_csv(file)
        except Exception as e:
            print(f"[SKIP] {file.name} — cannot read CSV ({e})")
            continue

        # --- normalize columns ---
        df.columns = [c.strip().lower() for c in df.columns]

        # 🔒 HARD GUARD — tylko snapshoty rynkowe
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

    ws.append_rows(rows_to_add, value_input_option="USER_ENTERED")
    print(f"[OK] Appended {len(rows_to_add)} new raw rows")


if __name__ == "__main__":
    main()