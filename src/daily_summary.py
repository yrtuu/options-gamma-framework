import pandas as pd
from pathlib import Path

DATA_PATH = Path("data/snapshots")

def summarize_symbol(symbol):
    files = sorted(DATA_PATH.glob(f"*_{symbol}.csv"))
    if len(files) < 2:
        return None

    df_prev = pd.read_csv(files[-2])
    df_today = pd.read_csv(files[-1])

    row = df_today.iloc[0]
    tags = structure_tags(row)
    close_vs_dnz = (
        "above DNZ" if row["spot"] > row["dnz_high"]
        else "below DNZ" if row["spot"] < row["dnz_low"]
        else "inside DNZ"
    )

    gamma_bias = (
        "gamma below > above" if row["gamma_below"] > row["gamma_above"]
        else "gamma above > below"
    )

    return f"""
{row['date']} {symbol}
Close {close_vs_dnz}
Gamma structure: {gamma_bias}
""".strip()

def main():
    for symbol in ["SPY", "QQQ", "AAPL"]:
        summary = summarize_symbol(symbol)
        if summary:
            print(summary)
            print("-" * 40)

if __name__ == "__main__":
    main()

def structure_tags(row):
    tags = []

    # DNZ position
    if row["spot"] > row["dnz_high"]:
        tags.append("break_above_dnz")
    elif row["spot"] < row["dnz_low"]:
        tags.append("break_below_dnz")
    else:
        tags.append("inside_dnz")

    # Gamma asymmetry
    if abs(row["gamma_above"] - row["gamma_below"]) < 1e6:
        tags.append("gamma_balanced")
    elif row["gamma_above"] > row["gamma_below"]:
        tags.append("gamma_asym_up")
    else:
        tags.append("gamma_asym_down")

    return "|".join(tags)
