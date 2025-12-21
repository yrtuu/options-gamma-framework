import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
from math import sqrt
from py_vollib.black_scholes.greeks.analytical import delta, gamma

SYMBOLS = ["SPY", "QQQ", "AAPL"]
RISK_FREE = 0.05

def dte_weight(dte):
    return 1 / sqrt(max(dte, 1))

def load_options(symbol):
    ticker = yf.Ticker(symbol)
    spot = ticker.history(period="1d")["Close"].iloc[-1]

    rows = []
    for exp in ticker.options:
        exp_date = datetime.strptime(exp, "%Y-%m-%d")
        dte = (exp_date - datetime.utcnow()).days
        if dte <= 0 or dte > 30:
            continue

        chain = ticker.option_chain(exp)
        for side, df in [("call", chain.calls), ("put", chain.puts)]:
            for _, r in df.iterrows():
                if r["openInterest"] > 0 and r["impliedVolatility"] > 0:
                    rows.append({
                        "strike": r["strike"],
                        "oi": r["openInterest"],
                        "iv": r["impliedVolatility"],
                        "dte": dte,
                        "type": side
                    })
    return spot, pd.DataFrame(rows)

def compute_greeks(df, spot):
    deltas, gammas = [], []

    for _, r in df.iterrows():
        flag = "c" if r["type"] == "call" else "p"
        try:
            d = delta(flag, spot, r["strike"], r["dte"]/365, RISK_FREE, r["iv"])
            g = gamma(flag, spot, r["strike"], r["dte"]/365, RISK_FREE, r["iv"])
        except:
            d, g = 0, 0

        w = dte_weight(r["dte"])
        deltas.append(d * r["oi"] * w)
        gammas.append(g * r["oi"] * w)

    df["delta_exp"] = deltas
    df["gamma_exp"] = gammas
    return df

def find_dnz(df, spot):
    prices = np.linspace(spot*0.9, spot*1.1, 200)
    totals = []

    for p in prices:
        totals.append(df["delta_exp"].sum())

    idx = np.argmin(np.abs(totals))
    mid = prices[idx]

    return mid*0.995, mid, mid*1.005

def run(symbol):
    spot, df = load_options(symbol)
    if df.empty:
        return

    df = compute_greeks(df, spot)

    gamma_above = df[df["strike"] > spot]["gamma_exp"].sum()
    gamma_below = df[df["strike"] < spot]["gamma_exp"].sum()

    # --- GAMMA DERIVED METRICS ---
    gamma_total = gamma_above + gamma_below
    gamma_diff = gamma_above - gamma_below
    gamma_ratio = gamma_above / gamma_total if gamma_total != 0 else 0.0
    gamma_asym_strength = abs(gamma_diff) / gamma_total if gamma_total != 0 else 0.0
    

    
    dnz_low, dnz_mid, dnz_high = find_dnz(df, spot)
    # --- SPOT POSITION IN DNZ ---
    dnz_range = dnz_high - dnz_low
    spot_position = (spot - dnz_mid) / dnz_range if dnz_range != 0 else 0.0

    
    today = datetime.utcnow().strftime("%Y-%m-%d")
    out = pd.DataFrame([{
    "date": today,
    "symbol": symbol,
    "spot": spot,
    "dnz_low": dnz_low,
    "dnz_mid": dnz_mid,
    "dnz_high": dnz_high,
    "spot_position": spot_position,

    "gamma_above": gamma_above,
    "gamma_below": gamma_below,
    "gamma_total": gamma_total,
    "gamma_diff": gamma_diff,
    "gamma_ratio": gamma_ratio,
    "gamma_asym_strength": gamma_asym_strength,

    # --- FUTURE PLACEHOLDERS ---
    "close_t+1": "",
    "close_t+2": "",
    "close_t+5": "",
    "event_flag": "",
}])


# ⬇️ JAWNIE WYBIERAMY KOLUMNY (KLUCZOWE)
    out = out[
    [
        "date",
        "symbol",
        "spot",
        "dnz_low",
        "dnz_mid",
        "dnz_high",
        "spot_position",

        "gamma_above",
        "gamma_below",
        "gamma_total",
        "gamma_diff",
        "gamma_ratio",
        "gamma_asym_strength",

        "close_t+1",
        "close_t+2",
        "close_t+5",
        "event_flag",
    ]
]


    out.to_csv(f"data/snapshots/{today}_{symbol}.csv", index=False)
if __name__ == "__main__":
    for s in SYMBOLS:
        run(s)
