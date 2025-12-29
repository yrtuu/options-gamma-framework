import os
import json
import pandas as pd
from datetime import datetime
from google.oauth2.service_account import Credentials
import gspread


SPREADSHEET_NAME = "Options Gamma Log"
RAW_SHEET = "raw_daily"
SUMMARY_SHEET = "daily_summary"


# ================= AUTH =================
def get_client():
    creds_json = json.loads(os.environ["GOOGLE_SHEETS_CREDENTIALS"])
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    return gspread.authorize(creds)


# ================= LOAD RAW (SAFE) =================
def load_raw():
    gc = get_client()
    ws = gc.open(SPREADSHEET_NAME).worksheet(RAW_SHEET)

    values = ws.get_all_values()
    if len(values) < 2:
        return pd.DataFrame(), ws, []

    headers = [h.strip().lower() for h in values[0]]
    rows = values[1:]

    df = pd.DataFrame(rows, columns=headers)
    return df, ws, headers


# ================= CORE LOGIC =================
def enrich_forward_metrics(df):
    df = df.copy()

    # --- typing ---
    df["spot"] = pd.to_numeric(df["spot"], errors="coerce")
    df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")

    df = df.dropna(subset=["spot", "date_dt"])
    df = df.sort_values(["symbol", "date_dt"])

    # --- init columns if missing ---
    for n in [1, 2, 5]:
        for col in [f"close_t+{n}", f"ret_t+{n}", f"days_to_close_t+{n}"]:
            if col not in df.columns:
                df[col] = ""

    # --- compute forward closes ---
    for symbol, g in df.groupby("symbol"):
        g = g.reset_index()

        for i, row in g.iterrows():
            base_idx = row["index"]

            for n in [1, 2, 5]:
                if i + n >= len(g):
                    continue

                # close
                if df.at[base_idx, f"close_t+{n}"] in ["", None]:
                    df.at[base_idx, f"close_t+{n}"] = g.loc[i + n, "spot"]

                # return
                try:
                    if df.at[base_idx, f"ret_t+{n}"] in ["", None]:
                        df.at[base_idx, f"ret_t+{n}"] = (
                            g.loc[i + n, "spot"] / row["spot"] - 1
                        )
                except Exception:
                    pass

                # horizon
                if df.at[base_idx, f"days_to_close_t+{n}"] in ["", None]:
                    df.at[base_idx, f"days_to_close_t+{n}"] = n

    return df.drop(columns=["date_dt"])


# ================= BATCH WRITE (NO QUOTA) =================
def batch_write(df, ws, headers):
    updates = []

    header_map = {h: i for i, h in enumerate(headers)}

    for row_idx, row in df.iterrows():
        sheet_row = row_idx + 2  # header offset

        for col in [
            "close_t+1", "close_t+2", "close_t+5",
            "ret_t+1", "ret_t+2", "ret_t+5",
            "days_to_close_t+1", "days_to_close_t+2", "days_to_close_t+5",
        ]:
            if col not in header_map:
                continue

            value = row.get(col, "")
            if value in ["", None]:
                continue

            col_idx = header_map[col] + 1
            updates.append({
                "range": gspread.utils.rowcol_to_a1(sheet_row, col_idx),
                "values": [[value]],
            })

    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")
        print(f"[OK] Updated {len(updates)} cells")
    else:
        print("[OK] Nothing to update")


# ================= DAILY SUMMARY =================
def write_daily_summary(df):
    df = df.copy()
    df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date_dt"])

    # ✅ REAL MARKET DATE (NOT "TODAY")
    last_market_date = df["date_dt"].max().date()
    day_df = df[df["date_dt"].dt.date == last_market_date]

    if day_df.empty:
        print("[SKIP] No rows for last market date")
        return

    counts = day_df["regime"].value_counts()
    dominant = counts.idxmax()
    share = round(counts.max() / counts.sum(), 2)
    symbols = int(counts.sum())

    gc = get_client()
    ws = gc.open(SPREADSHEET_NAME).worksheet(SUMMARY_SHEET)

    values = ws.get_all_values()
    if values:
        existing_dates = {r[0] for r in values[1:] if r}
        if last_market_date.strftime("%Y-%m-%d") in existing_dates:
            print(f"[SKIP] daily_summary already exists for {last_market_date}")
            return
    else:
        ws.append_row(
            ["date", "dominant_regime", "share", "symbols"],
            value_input_option="RAW",
        )

    ws.append_row(
        [
            last_market_date.strftime("%Y-%m-%d"),
            dominant,
            share,
            symbols,
        ],
        value_input_option="RAW",
    )

    print(f"[OK] daily_summary added for {last_market_date}")


# ================= ENTRY =================
def main():
    df, ws, headers = load_raw()

    print("RAW_DAILY columns:", headers)

    if df.empty or "date" not in df.columns or "symbol" not in df.columns:
        print("No valid data — skipping postprocess")
        return

    df = enrich_forward_metrics(df)
    batch_write(df, ws, headers)
    write_daily_summary(df)


if __name__ == "__main__":
    main()