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
