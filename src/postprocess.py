import os
import json
import numpy as np
import pandas as pd
import gspread
from datetime import datetime
from google.oauth2.service_account import Credentials
from pathlib import Path

# ================= CONFIG =================
SPREADSHEET_NAME = "Options Gamma Log"
RAW_SHEET = "raw_daily"
SUMMARY_SHEET = "daily_summary"
CALENDAR_PATH = Path("data/calendars")

# ================= AUTH =================
def get_client():
    creds_json = json.loads(os.environ["GOOGLE_SHEETS_CREDENTIALS"])
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    return gspread.authorize(creds)

# ================= LOAD RAW =================
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

# ================= EVENT CALENDAR =================
def load_event_calendar():
    events = {}
    for file in ["fomc.csv", "cpi.csv", "opex.csv"]:
        path = CALENDAR_PATH / file
        if not path.exists():
            continue
        df = pd.read_csv(path)
        for _, r in df.iterrows():
            events[str(r["date"])] = r["event"]
    return events

def resolve_event(market_date, events):
    if market_date in events:
        return True, events[market_date]
    return False, "NONE"

def resolve_event_phase(market_date, events):
    if not events:
        return "NONE"

    market_dt = pd.to_datetime(market_date)
    event_dates = sorted(pd.to_datetime(list(events.keys())))

    if market_dt in event_dates:
        return "EVENT"

    for d in event_dates:
        if market_dt < d:
            return "PRE_EVENT"

    return "POST_EVENT"

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

                if df.at[base_idx, f"ret_t+{n}"] in ["", None]:
                    df.at[base_idx, f"ret_t+{n}"] = future_spot / row["spot"] - 1

                if df.at[base_idx, f"days_to_close_t+{n}"] in ["", None]:
                    df.at[base_idx, f"days_to_close_t+{n}"] = n

    df["data_ok"] = (
        df.groupby("date")["symbol"]
        .transform("nunique")
        .ge(3)
    )

    return df.drop(columns=["date_dt"])

# ================= NUMERIC CAST =================
def cast_numeric(df):
    num_cols = [
        "ret_t+1", "dnz_width", "spot_position",
        "effective_gamma_pressure", "gamma_asym_strength"
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

# ================= KROK A — INTRADAY STRUCTURE =================
def add_intraday_structure(df):
    df["day_direction"] = np.where(
        df["ret_t+1"] > 0, "UP",
        np.where(df["ret_t+1"] < 0, "DOWN", "FLAT")
    )

    df["range_expansion"] = (
        df.groupby("symbol")["dnz_width"]
        .transform(lambda x: x > x.rolling(5, min_periods=1).median())
    )

    df["close_location"] = np.where(
        df["spot_position"] > 0.5, "HIGH",
        np.where(df["spot_position"] < -0.5, "LOW", "MID")
    )

    return df

# ================= KROK B — STREAKS =================
def compute_streak(series):
    return (
        series.groupby((series != series.shift()).cumsum())
        .cumcount() + 1
    )

def add_streaks(df):
    df = df.sort_values(["symbol", "date"])
    df["spot_bucket_streak"] = df.groupby("symbol")["spot_bucket"].transform(compute_streak)
    df["gamma_bucket_streak"] = df.groupby("symbol")["gamma_bucket"].transform(compute_streak)
    df["regime_streak"] = df.groupby("symbol")["regime"].transform(compute_streak)
    return df

# ================= KROK C — CROSS SYMBOL =================
def add_cross_symbol(df):
    df["symbols_same_spot_bucket"] = (
        df.groupby("date")["spot_bucket"]
        .transform(lambda x: x.value_counts().max())
    )

    df["symbols_same_gamma_bucket"] = (
        df.groupby("date")["gamma_bucket"]
        .transform(lambda x: x.value_counts().max())
    )

    df["cross_symbol_alignment"] = np.select(
        [
            df["symbols_same_gamma_bucket"] >= 3,
            df["symbols_same_gamma_bucket"] == 2,
        ],
        ["HIGH", "MEDIUM"],
        default="LOW"
    )

    return df

# ================= KROK D — EVENT × STRUCTURE =================
def add_event_structure(df):
    median_egp = df["effective_gamma_pressure"].median()

    df["event_structure_tag"] = (
        df["event_phase"] + " | " +
        df["gamma_bucket"] + " | " +
        np.where(df["effective_gamma_pressure"] > median_egp, "high_egp", "low_egp")
    )

    df["event_risk_flag"] = np.select(
        [
            (df["event_phase"] == "EVENT") & (df["effective_gamma_pressure"] < 1e-4),
            (df["event_phase"] == "PRE_EVENT") & (df["gamma_asym_strength"] > 0.3),
        ],
        ["AVOID", "FAVORABLE"],
        default="NEUTRAL"
    )

    return df

# ================= KROK E — REGIME QUALITY =================
def add_regime_quality(df):
    df["regime_quality_score"] = (
        (df["regime_streak"] >= 2).astype(int)
        + (df["symbols_same_gamma_bucket"] >= 2).astype(int)
        + (df["range_expansion"] == True).astype(int)
        - (
            (df["event_phase"] == "EVENT")
            & (df["effective_gamma_pressure"] < 1e-4)
        ).astype(int)
    )
    return df



# ================= SANITIZE FOR GOOGLE SHEETS =================
def sanitize_for_sheets(df):
    df = df.replace([np.inf, -np.inf], np.nan)

    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna("")
        else:
            df[col] = df[col].fillna("")

    return df


# ================= WRITE BACK (SAFE) =================
def batch_write(df, ws, headers):
    header_map = {h: i for i, h in enumerate(headers)}
    updates = []

    for idx, row in df.iterrows():
        sheet_row = idx + 2
        updated = ws.row_values(sheet_row)
        updated += [""] * (len(headers) - len(updated))

        for col, val in row.items():
            if col not in header_map:
                continue
            if val in ["", None]:
                continue
            updated[header_map[col]] = val

        updates.append({
            "range": f"A{sheet_row}",
            "values": [updated],
        })

    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")
        print(f"[OK] Updated {len(updates)} rows")


def write_daily_summary(df):
    df = df.copy()

    df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
    last_market_date = df["date_dt"].max().date()

    day_df = df[df["date_dt"].dt.date == last_market_date]
    if day_df.empty:
        return

    counts = day_df["regime"].value_counts()
    dominant = counts.idxmax()
    share = round(counts.max() / counts.sum(), 2)

    summary_df, ws = load_summary_df()
    if not summary_df.empty:
        summary_df["date_dt"] = pd.to_datetime(summary_df["date"], errors="coerce")
        if last_market_date <= summary_df["date_dt"].max().date():
            return

    # ⏱️ TIMESTAMP PIPELINE (UTC)
    created_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # HEADER (tylko gdy arkusz pusty)
    if ws.get_all_values() == []:
        ws.append_row(
            ["date", "dominant_regime", "share", "symbols", "created_at_utc"],
            value_input_option="RAW"
        )

    ws.append_row(
        [
            last_market_date.strftime("%Y-%m-%d"),
            dominant,
            share,
            len(day_df),
            created_at,
        ],
        value_input_option="RAW"
    )   

    

# ================= ENTRY =================
def main():
    df, ws, headers = load_raw()
    if df.empty:
        return

    df = enrich_forward_metrics(df)
    df = cast_numeric(df)

    # EVENTS
    events = load_event_calendar()
    df["is_event_day"] = False
    df["event_type"] = "NONE"
    df["event_phase"] = "NONE"

    for date, g in df.groupby("date"):
        is_event, event_type = resolve_event(date, events)
        event_phase = resolve_event_phase(date, events)
        df.loc[g.index, "is_event_day"] = is_event
        df.loc[g.index, "event_type"] = event_type
        df.loc[g.index, "event_phase"] = event_phase

    # === INSTITUTIONAL BLOCKS ===
    df = add_intraday_structure(df)
    df = add_streaks(df)
    df = add_cross_symbol(df)
    df = add_event_structure(df)
    df = add_regime_quality(df)

    df = sanitize_for_sheets(df)
   
     
    batch_write(df, ws, headers)
    write_daily_summary(df)

if __name__ == "__main__":
    main()