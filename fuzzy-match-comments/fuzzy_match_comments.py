import os
import sqlite3
import zipfile
import pandas as pd
from rapidfuzz import fuzz
import re

# ------------------------------------------
# CONFIG
# ------------------------------------------
ZIP_PATH = "/Users/dorajyl/Desktop/w/UROP/Part 2. Data Cleaning/based_on_comment/comments before clean.zip"
CSV_PATH = "/Users/dorajyl/Desktop/w/UROP/Part 2. Data Cleaning/based_on_comment/sp500_financial_firms_with_tickers_1994_2019.csv"

MATCH_THRESHOLD = 82  # slightly lower to allow multiple true matches

# ------------------------------------------
# CLEAN COMPANY NAMES FOR MATCHING
# ------------------------------------------
def normalize_name(name):
    name = name.upper()
    name = re.sub(r"[^\w\s]", " ", name)  # remove punctuation
    name = re.sub(r"\b(CORP(ORATION)?|INC(ORPORATED)?|CO(MPANY)?|LLC|LTD|HOLDINGS?)\b", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name

df = pd.read_csv(CSV_PATH)
df["name_clean"] = df["name"].apply(normalize_name)
df["ticker_clean"] = df["ticker"].astype(str).str.upper().str.strip()

firm_pairs = list(zip(df["name_clean"], df["ticker_clean"]))

print(f"Loaded {len(firm_pairs)} firms (name+ticker pairs, normalized for matching).")

# ------------------------------------------
# DATABASE (resume safe)
# ------------------------------------------
conn = sqlite3.connect("financial_matches.db")
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS matches (
    filename TEXT,
    firm_name TEXT,
    ticker TEXT,
    score INTEGER
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS processed (
    filename TEXT PRIMARY KEY
);
""")


# ------------------------------------------
# PREFIX CHECK
# ------------------------------------------
def valid_prefix(fname):
    prefix = fname.split("-")[0].upper()
    return prefix in ("34", "IC", "IA")

# ------------------------------------------
# PROCESS ZIP
# ------------------------------------------
with zipfile.ZipFile(ZIP_PATH, "r") as z:
    txt_files = [f for f in z.namelist() if f.lower().endswith(".txt")]

    for filename in txt_files:

        base = os.path.basename(filename)

        if not valid_prefix(base):
            continue

        cur.execute("SELECT 1 FROM processed WHERE filename=?", (base,))
        if cur.fetchone():
            print(f"⏩ Skipping (done): {base}")
            continue

        print(f"🔍 Processing: {base}")

        try:
            with z.open(filename) as f:
                text = f.read().decode("utf-8", errors="ignore").upper()
        except Exception as e:
            print(f"⚠️ Error reading {base}: {e}")
            continue

        # Check **every firm** (no stopping early — this ensures multiple matches occur)
        for name_clean, ticker in firm_pairs:
            score = fuzz.partial_ratio(name_clean, text)
            if score >= MATCH_THRESHOLD:
                cur.execute("""
                    INSERT INTO matches (filename, firm_name, ticker, score)
                    VALUES (?, ?, ?, ?)
                """, (base, name_clean, ticker, score))

        cur.execute("INSERT OR IGNORE INTO processed (filename) VALUES (?)", (base,))
        conn.commit()

conn.close()
print("✅ DONE. All matches stored. Safe to interrupt and resume anytime.")
