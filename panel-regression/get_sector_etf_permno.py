# get_sector_etf_permno.py

import pandas as pd
import wrds

# 1. Connect to WRDS
db = wrds.Connection()

# 2. Your 12 sector ETF tickers
sector_tickers = [
    "XLB",  # Materials
    "XLC",  # Communication Services
    "XLE",  # Energy
    "XLF",  # Financials
    "XLI",  # Industrials
    "XLK",  # Technology
    "XLP",  # Consumer Staples
    "XLRE", # Real Estate
    "XLU",  # Utilities
    "XLV",  # Health Care
    "XLY",  # Consumer Discretionary
    "SPY",  # Broad market (if you’re using this as #12)
]

ticker_list = ",".join(f"'{t}'" for t in sector_tickers)

# 3. Pull from CRSP stocknames (this table *does* have ticker)
query = f"""
    SELECT permno, ticker, comnam, namedt, nameenddt
    FROM crsp.stocknames
    WHERE ticker IN ({ticker_list})
"""
names = db.raw_sql(query)

print("Raw matches from crsp.stocknames:")
print(names.sort_values(['ticker', 'namedt']).head(40))

# 4. For each ticker, keep the latest name record (gives one permno per ticker)
names_sorted = names.sort_values(['ticker', 'namedt'])
latest = (
    names_sorted
    .groupby('ticker', as_index=False)
    .tail(1)   # last record per ticker
)

sector_permno = (
    latest[['ticker', 'permno', 'comnam']]
    .sort_values('ticker')
    .reset_index(drop=True)
)

print("\nFinal ETF-level mapping:")
print(sector_permno)

# 5. Save to CSV
out_path = "sector_permno.csv"
sector_permno.to_csv(out_path, index=False)
print(f"\nSaved ETF permnos to: {out_path}")
