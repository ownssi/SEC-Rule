import wrds
import pandas as pd

START = "1994-01-01"
END   = "2019-12-31"

db = wrds.Connection()

# 1) Extract unique firms whose membership period overlaps the window
sp500_firms = db.raw_sql(f"""
    SELECT DISTINCT permno
    FROM crsp.dsp500list
    WHERE start <= '{END}' AND (ending >= '{START}' OR ending IS NULL)
""")

# 2) Time-varying firm names and SIC
names = db.raw_sql("""
    SELECT permno, comnam, siccd, namedt, nameendt
    FROM crsp.msenames
""", date_cols=['namedt','nameendt'])

# 3) Time-varying tickers
tickers = db.raw_sql("""
    SELECT permno, ticker, namedt AS tic_start, nameenddt AS tic_end
    FROM crsp.stocknames
""", date_cols=['tic_start','tic_end'])

# 4) Merge
merged = (sp500_firms
          .merge(names, on='permno', how='left')
          .merge(tickers, on='permno', how='left'))

# 5) Keep financial firms (SIC 6000–6999)
merged = merged[(merged['siccd'] >= 6000) & (merged['siccd'] < 7000)]

# 6) Collapse to one row per firm
def mode_or_last(x):
    m = x.mode()
    return m.iloc[0] if len(m) > 0 else x.dropna().iloc[-1] if x.dropna().size > 0 else None

unique_firms = merged.groupby('permno').agg(
    name=('comnam', lambda s: s.dropna().iloc[-1]),
    sic=('siccd', mode_or_last),
    ticker=('ticker', lambda s: s.dropna().iloc[-1]),
).reset_index()

unique_firms = unique_firms.sort_values('permno')

unique_firms.to_csv("sp500_financial_firms_with_tickers_1994_2019.csv", index=False)

print(f"Total firms: {len(unique_firms)}")
print(unique_firms.head(10))
