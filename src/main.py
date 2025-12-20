from datetime import datetime
from pathlib import Path
import yfinance as yf
import pandas as pd

# folder na dane
out_dir = Path("data/snapshots")
out_dir.mkdir(parents=True, exist_ok=True)

# czas
now = datetime.utcnow().strftime("%Y-%m-%d")

# ticker
ticker = yf.Ticker("SPY")

# dane dzienne
hist = ticker.history(period="5d")

# zapis
filename = out_dir / f"spy_{now}.csv"
hist.to_csv(filename)

print(f"Saved {filename}")

