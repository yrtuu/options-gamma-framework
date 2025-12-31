import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
from math import sqrt
from py_vollib.black_scholes.greeks.analytical import delta, gamma
# ================= NYSE CALENDAR =================
import pandas_market_calendars as mcal
import pandas as pd
from datetime import datetime

nyse = mcal.get_calendar("NYSE")

def get_last_market_date():
    """
    Zwraca ostatni dzień handlowy NYSE.
    Uwzględnia weekendy, święta i half-days.
    """
    today = datetime.utcnow().date()
    schedule = nyse.schedule(
        start_date=today - pd.Timedelta(days=7),
        end_date=today
    )

    if schedule.empty:
        return today.strftime("%Y-%m-%d")

    last_session = schedule.index[-1]
    return last_session.strftime("%Y-%m-%d")

# ================= BUCKETS =================
def spot_bucket(x):
    if abs(x) < 0.3:
        return "center"
    if x > 1:
        return "break_up"
    if x < -1:
        return "break_down"
    return "edge"


def gamma_bucket(r):
    if r > 0.6:
        return "gamma_up"
    if r < 0.4:
        return "gamma_down"
    return "gamma_neutral"


def week_from_date(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year, week, _ = dt.isocalendar()
    return f"{year}-{week:02d}"


# ================= CONFIG =================
SYMBOLS = ["SPY", "QQQ", "AAPL"]
RISK_FREE = 0.05


def dte_weight(dte):
    return 1 / sqrt(max(dte, 1))


# ================= OPTIONS LOAD =================
def load_options(symbol):
    ticker = yf.Ticker(symbol)

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
                        "type": side,
                    })

    return pd.DataFrame(rows)


# ================= GREEKS =================
def compute_greeks(df, spot):
    deltas, gammas = [], []

    for _, r in df.iterrows():
        flag = "c" if r["type"] == "call" else "p"
        try:
            d = delta(flag, spot, r["strike"], r["dte"] / 365, RISK_FREE, r["iv"])
            g = gamma(flag, spot, r["strike"], r["dte"] / 365, RISK_FREE, r["iv"])
        except Exception:
            d, g = 0.0, 0.0

        w = dte_weight(r["dte"])
        deltas.append(d * r["oi"] * w)
        gammas.append(g * r["oi"] * w)

    df["delta_exp"] = deltas
    df["gamma_exp"] = gammas
    return df


# ================= GAMMA PROFILE =================
def compute_gamma_profile(df, spot):
    gamma_by_strike = (
        df.groupby("strike")["gamma_exp"]
        .sum()
        .sort_index()
    )

    if gamma_by_strike.empty:
        return {
            "gamma_peak_price": np.nan,
            "gamma_concentration": 0.0,
            "gamma_distance_from_spot": 0.0,
        }

    gamma_peak_price = gamma_by_strike.abs().idxmax()
    total_gamma = gamma_by_strike.abs().sum()

    top_gamma = gamma_by_strike.abs().nlargest(3).sum()
    gamma_concentration = top_gamma / total_gamma if total_gamma != 0 else 0.0

    gamma_distance_from_spot = (gamma_peak_price - spot) / spot

    return {
        "gamma_peak_price": gamma_peak_price,
        "gamma_concentration": gamma_concentration,
        "gamma_distance_from_spot": gamma_distance_from_spot,
    }


# ================= DNZ =================
def find_dnz(df, spot):
    prices = np.linspace(spot * 0.9, spot * 1.1, 200)
    net_deltas = []

    for p in prices:
        total = 0.0
        for _, r in df.iterrows():
            flag = "c" if r["type"] == "call" else "p"
            try:
                d = delta(flag, p, r["strike"], r["dte"] / 365, RISK_FREE, r["iv"])
            except Exception:
                d = 0.0

            w = dte_weight(r["dte"])
            total += d * r["oi"] * w

        net_deltas.append(total)

    net_deltas = np.array(net_deltas)
    idx = np.argmin(np.abs(net_deltas))
    dnz_mid = prices[idx]

    width = (prices.max() - prices.min()) * 0.005
    return dnz_mid - width, dnz_mid, dnz_mid + width


# ================= EGP =================
def compute_effective_gamma_pressure(df, spot, eps_pct=0.002):
    eps = spot * eps_pct

    def net_delta(p):
        total = 0.0
        for _, r in df.iterrows():
            flag = "c" if r["type"] == "call" else "p"
            try:
                d = delta(flag, p, r["strike"], r["dte"] / 365, RISK_FREE, r["iv"])
            except Exception:
                d = 0.0

            w = dte_weight(r["dte"])
            total += d * r["oi"] * w
        return total

    return abs(net_delta(spot + eps) - net_delta(spot - eps)) / (2 * eps)


# ================= MAIN RUN =================
def run(symbol):
    ticker = yf.Ticker(symbol)

    # ✅ SOURCE OF TRUTH: NYSE CALENDAR
hist = ticker.history(period="5d")
if hist.empty:
    return

last_bar = hist.index[-1]
spot = hist["Close"].iloc[-1]

# --- użyj oficjalnego NYSE kalendarza ---
market_date = get_last_market_date()

    options_df = load_options(symbol)
    if options_df.empty:
        return

    options_df = compute_greeks(options_df, spot)
    gamma_profile = compute_gamma_profile(options_df, spot)

    gamma_above = options_df[options_df["strike"] > spot]["gamma_exp"].sum()
    gamma_below = options_df[options_df["strike"] < spot]["gamma_exp"].sum()

    gamma_total = gamma_above + gamma_below
    gamma_diff = gamma_above - gamma_below
    gamma_ratio = gamma_above / gamma_total if gamma_total != 0 else 0.0
    gamma_asym_strength = abs(gamma_diff) / gamma_total if gamma_total != 0 else 0.0

    dnz_low, dnz_mid, dnz_high = find_dnz(options_df, spot)
    effective_gamma_pressure = compute_effective_gamma_pressure(options_df, spot)

    dnz_range = dnz_high - dnz_low
    spot_position = (spot - dnz_mid) / dnz_range if dnz_range != 0 else 0.0

    sb = spot_bucket(spot_position)
    gb = gamma_bucket(gamma_ratio)
    regime = f"{sb} | {gb}"

    out = pd.DataFrame([{
        "date": market_date,
        "week": week_from_date(market_date),
        "symbol": symbol,
        "spot": spot,

        "dnz_low": dnz_low,
        "dnz_mid": dnz_mid,
        "dnz_high": dnz_high,
        "dnz_width": dnz_high - dnz_low,
        "spot_position": spot_position,

        "spot_bucket": sb,
        "gamma_bucket": gb,
        "regime": regime,

        "gamma_above": gamma_above,
        "gamma_below": gamma_below,
        "gamma_total": gamma_total,
        "gamma_diff": gamma_diff,
        "gamma_ratio": gamma_ratio,
        "gamma_asym_strength": gamma_asym_strength,

        "effective_gamma_pressure": effective_gamma_pressure,
        "egp_normalized": effective_gamma_pressure / (dnz_high - dnz_low + 1e-9),

        "gamma_peak_price": gamma_profile["gamma_peak_price"],
        "gamma_concentration": gamma_profile["gamma_concentration"],
        "gamma_distance_from_spot": gamma_profile["gamma_distance_from_spot"],

        "close_t+1": "",
        "close_t+2": "",
        "close_t+5": "",
        "event_flag": "",
    }])

    for col in out.columns:
        if pd.api.types.is_numeric_dtype(out[col]):
            out[col] = out[col].astype(float)

# --- SAFE GUARD: nie zapisuj daty z przyszłości ---
today = datetime.utcnow().date()
if pd.to_datetime(market_date).date() > today:
    print(f"[SKIP] {symbol} — future market date {market_date}")
    return 

    out.to_csv(
        f"data/snapshots/{market_date}_{symbol}.csv",
        index=False,
        float_format="%.10f",
    )


if __name__ == "__main__":
    for s in SYMBOLS:
        run(s)