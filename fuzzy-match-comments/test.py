import sqlite3
import pandas as pd

DB_PATH = "financial_matches.db"
OUTPUT_CSV = "financial_matches_full.csv"

conn = sqlite3.connect(DB_PATH)

df = pd.read_sql_query("SELECT * FROM matches ORDER BY filename", conn)

df.to_csv(OUTPUT_CSV, index=False)

conn.close()

print(f"✅ Exported to {OUTPUT_CSV}")
