import pandas as pd
from pathlib import Path

DATA_PATH = Path("data/snapshots")

def structure_tags(row):
    tags = []

    # --- DNZ position ---
    if row["spot"] > row["dnz_high"]:
        tags.append("break_above_dnz")
    elif row["spot"] < row["dnz_low"]:
        tags.append("break_below_dnz")
    else:
        tags.append("inside_dnz")

    # --- Gamma asymmetry ---
    if abs(row["gamma_above"] - row["gamma_below"]) < 1e-6:
        tags.append("gamma_balanced")
    elif row["gamma_above"] > row["gamma_below"]:
        tags.append("gamma_asym_up")
    else:
        tags.append("gamma_asym_down")

    # --- Effective Gamma Pressure ---
    egp = row.get("effective_gamma_pressure", 0)

    if egp > 1e-4:
        tags.append("high_egp")
    else:
        tags.append("low_egp")

    return " | ".join(tags)


def summarize_symbol(symbol):
    files = sorted(DATA_PATH.glob(f"*_{symbol}.csv"))
    if not files:
        return None

    df_today = pd.read_csv(files[-1])
    row = df_today.iloc[0]

    return {
        "date": row["date"],
        "symbol": symbol,
        "structure_tags": structure_tags(row),
    }  


