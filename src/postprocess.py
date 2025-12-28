import os
import json
import pandas as pd
import gspread
from datetime import datetime
from google.oauth2.service_account import Credentials


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


# ================= LOAD SUMMARY =================
def load_summary_df():
    gc = get_client()
    ws = gc.open(SPREADSHEET_NAME).worksheet(SUMMARY_SHEET)

    values = ws.get_all_values()
    if len(values) < 2:
        return pd.DataFrame(), ws

    headers = [h.strip().lower() for h in values[0]]
    rows = values[1:]

    df = pd.DataFrame(rows, columns=headers)
    return df, ws


# ================= FORWARD METRICS =================
def enrich_forward_metrics(df):
    df = df.copy()

    df["spot"] = pd.to_numeric(df["spot"], errors="coerce")
    df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")

    df = df.dropna(subset=["spot", "date_dt"])
    df = df.sort_values(["symbol", "date_dt"])

    for n in [1, 2, 5]:
        for col in [f"close_t+{n}", f"ret_t+{n}", f"days_to_close_t+{n}"]:
            if col not in df.columns:
                df[col] = ""

    for symbol, g in df.groupby("symbol"):
        g = g.reset_index()

        for i, row in g.iterrows():
            base_idx = row["index"]

            for n in [1, 2, 5]:
                if i + n >= len(g):
                    continue

                future_spot = g.loc[i + n, "spot"]

                if df.at[base_idx, f"close_t+{n}"] in ["", None]:
                    df.at[base_idx, f"close_t+{n}"] = future_spot

                try:
                    if df.at[base_idx, f"ret_t+{n}"] in ["", None]:
                        df.at[base_idx, f"ret_t+{n}"] = future_spot / row["spot"] - 1
                except Exception:
                    pass

                if df.at[base_idx, f"days_to_close_t+{n}"] in ["", None]:
                    df.at[base_idx, f"days_to_close_t+{n}"] = n

   # ================= DATA QUALITY FLAG =================
    MIN_SYMBOLS = 3

    df["data_ok"] = (
        df.groupby("date")["symbol"]
          .transform("nunique")
          .ge(MIN_SYMBOLS)
    )

    return df.drop(columns=["date_dt"])


# ================= WRITE BACK RAW =================
def batch_write(df, ws, headers):
    header_map = {h: i for i, h in enumerate(headers)}
    updates = []

    for idx, row in df.iterrows():
        sheet_row = idx + 2

        updated = ws.row_values(sheet_row)
        updated += [""] * (len(headers) - len(updated))

        for col in [
            "close_t+1", "close_t+2", "close_t+5",
            "ret_t+1", "ret_t+2", "ret_t+5",
            "days_to_close_t+1", "days_to_close_t+2", "days_to_close_t+5",
      "data_ok",  ]:
            if col not in header_map:
                continue

            val = row.get(col, "")
            if val in ["", None]:
                continue

            updated[header_map[col]] = val

        updates.append((sheet_row, updated))

    for r, values in updates:
        ws.update(f"A{r}", [values], value_input_option="USER_ENTERED")

    print(f"[OK] Updated {len(updates)} raw rows")


# ================= DAILY SUMMARY (HARD GUARD) =================
def write_daily_summary(df):
    df = df.copy()
    df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date_dt"])

    last_market_date = df["date_dt"].max().date()
    day_df = df[df["date_dt"].dt.date == last_market_date]

    if day_df.empty:
        print("[SKIP] No rows for last market date")
        return

    counts = day_df["regime"].value_counts()
    dominant = counts.idxmax()
    share = round(counts.max() / counts.sum(), 2)
    symbols = int(counts.sum())

    summary_df, ws = load_summary_df()

    if not summary_df.empty:
        summary_df["date_dt"] = pd.to_datetime(summary_df["date"], errors="coerce")
        last_summary_date = summary_df["date_dt"].max().date()

        # 🔒 NAJWAŻNIEJSZY GUARD
        if last_market_date <= last_summary_date:
            print(f"[SKIP] No new market session (last={last_market_date})")
            return

    if ws.get_all_values() == []:
        ws.append_row(
            ["date", "dominant_regime", "share", "symbols"],
            value_input_option="RAW",
        )

    ws.append_row(
        [last_market_date.strftime("%Y-%m-%d"), dominant, share, symbols],
        value_input_option="RAW",
    )

    print(f"[OK] daily_summary added for {last_market_date}")


# ================= ENTRY =================
def main():
    df, ws, headers = load_raw()

    print("RAW_DAILY columns:", headers)

    if df.empty or "date" not in df.columns or "symbol" not in df.columns:
        print("[EXIT] No valid raw data")
        return

    df = enrich_forward_metrics(df)
    batch_write(df, ws, headers)
    write_daily_summary(df)


if __name__ == "__main__":
    main()