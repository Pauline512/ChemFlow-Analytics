"""
read_data.py
============
Quick sanity check after running 01_download_data.py.
Loads all 4 raw CSV files and prints their shape and columns.
Run this BEFORE 02_clean_data.py to confirm downloads succeeded.

Usage:
    python read_data.py
"""

import pandas as pd
import os

print("=" * 55)
print("  ChemFlow Analytics — Raw Data Verification Check")
print("  Topic: Chemistry Patents (CPC Section C)")
print("=" * 55)

files = {
    "raw_patents":       "data/raw_patents.csv",
    "raw_inventors":     "data/raw_inventors.csv",
    "raw_companies":     "data/raw_companies.csv",
    "raw_relationships": "data/raw_relationships.csv",
}

all_good = True

for name, path in files.items():
    if os.path.exists(path):
        df = pd.read_csv(path)
        rows, cols = df.shape
        size_kb = os.path.getsize(path) / 1024
        status = "✔" if rows > 0 else "⚠ EMPTY"
        print(f"\n  {status} {name}")
        print(f"     Rows    : {rows:,}")
        print(f"     Columns : {cols} → {list(df.columns)}")
        print(f"     Size    : {size_kb:.1f} KB")
        if rows == 0:
            all_good = False
    else:
        print(f"\n  ✘ {name}")
        print(f"     NOT FOUND at {path}")
        print(f"     → Run: python scripts/01_download_data.py first")
        all_good = False

print("\n" + "=" * 55)
if all_good:
    print("  All files loaded successfully.")
    print("  Next step: python scripts/02_clean_data.py")
else:
    print("  Some files are missing or empty.")
    print("  Re-run: python scripts/01_download_data.py")
print("=" * 55)