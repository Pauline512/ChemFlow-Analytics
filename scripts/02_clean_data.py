"""
02_clean_data.py
================
Cleans raw patent data produced by 01_download_data.py.
Uses pandas to fix missing values, duplicates, formatting.

Reads from:  data/raw_*.csv
Writes to:   data/clean_*.csv
             reports/*_report.txt
"""

import pandas as pd
import os

# ── SETUP ─────────────────────────────────────────────────────────────────────
os.makedirs("reports",  exist_ok=True)
os.makedirs("database", exist_ok=True)   # needed by 03_load_database.py later
os.makedirs("data",     exist_ok=True)

def log(msg):
    print(f"[INFO] {msg}")

def save_report(name, df):
    path = f"reports/{name}_report.txt"
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"{name.upper()} CLEANING REPORT\n")
        f.write("=" * 40 + "\n")
        f.write(f"Rows    : {df.shape[0]:,}\n")
        f.write(f"Columns : {df.shape[1]}\n\n")
        f.write("Missing values per column:\n")
        f.write(str(df.isnull().sum()))
        f.write("\n\nSample (first 3 rows):\n")
        f.write(df.head(3).to_string())
    log(f"Report saved → {path}")


# ── LOAD ──────────────────────────────────────────────────────────────────────
log("Loading raw datasets...")
patents       = pd.read_csv("data/raw_patents.csv")
inventors     = pd.read_csv("data/raw_inventors.csv")
companies     = pd.read_csv("data/raw_companies.csv")
relationships = pd.read_csv("data/raw_relationships.csv")

log(f"  raw_patents       : {len(patents):,} rows")
log(f"  raw_inventors     : {len(inventors):,} rows")
log(f"  raw_companies     : {len(companies):,} rows")
log(f"  raw_relationships : {len(relationships):,} rows")


# ── GENERIC CLEANER ───────────────────────────────────────────────────────────
def clean_basic(df):
    df = df.copy()
    df = df.drop_duplicates()
    df.columns = df.columns.str.lower().str.strip()
    return df


# ── PATENTS ───────────────────────────────────────────────────────────────────
log("Cleaning patents...")
patents = clean_basic(patents)

patents = patents.dropna(subset=["patent_id", "title", "filing_date"])
patents["title"]    = patents["title"].str.title().str.strip()
patents["abstract"] = patents["abstract"].fillna("No abstract available").str.strip()

patents["year"] = pd.to_numeric(patents["year"], errors="coerce")
patents = patents.dropna(subset=["year"])
patents["year"] = patents["year"].astype(int)

patents["filing_date"] = pd.to_datetime(patents["filing_date"], errors="coerce")
patents = patents.dropna(subset=["filing_date"])
patents["filing_date"] = patents["filing_date"].dt.date

log(f"  Clean patents: {len(patents):,} rows")
save_report("patents", patents)


# ── INVENTORS ─────────────────────────────────────────────────────────────────
log("Cleaning inventors...")
inventors = clean_basic(inventors)

inventors = inventors.dropna(subset=["inventor_id", "name"])
inventors["name"]    = inventors["name"].str.title().str.strip()
inventors["country"] = inventors["country"].fillna("UNKNOWN").str.upper().str.strip()

# ── FIX: keep only inventors that appear in relationships ─────────────────────
# This prevents the relationships table from being emptied in the filter step
linked_inv_ids = set(relationships["inventor_id"].dropna().astype(str).unique())
inventors_linked = inventors[inventors["inventor_id"].astype(str).isin(linked_inv_ids)]

if len(inventors_linked) > 0:
    log(f"  Chemistry-linked inventors: {len(inventors_linked):,} (filtered from {len(inventors):,})")
    inventors = inventors_linked
else:
    log("  WARNING: No inventor ID overlap found — keeping all inventors")

log(f"  Clean inventors: {len(inventors):,} rows")
save_report("inventors", inventors)


# ── COMPANIES ─────────────────────────────────────────────────────────────────
log("Cleaning companies...")
companies = clean_basic(companies)

companies = companies.dropna(subset=["company_id", "name"])
companies["name"] = companies["name"].str.title().str.strip()

# ── FIX: keep only companies that appear in relationships ─────────────────────
linked_co_ids = set(relationships["company_id"].dropna().astype(str).unique())
companies_linked = companies[companies["company_id"].astype(str).isin(linked_co_ids)]

if len(companies_linked) > 0:
    log(f"  Chemistry-linked companies: {len(companies_linked):,} (filtered from {len(companies):,})")
    companies = companies_linked
else:
    log("  WARNING: No company ID overlap found — keeping all companies")

log(f"  Clean companies: {len(companies):,} rows")
save_report("companies", companies)


# ── RELATIONSHIPS ─────────────────────────────────────────────────────────────
log("Cleaning relationships...")
relationships = clean_basic(relationships)
relationships = relationships.dropna()

# Convert IDs to string for consistent matching
relationships["patent_id"]   = relationships["patent_id"].astype(str)
relationships["inventor_id"]  = relationships["inventor_id"].astype(str)
relationships["company_id"]   = relationships["company_id"].astype(str)

before = len(relationships)

relationships = relationships[
    relationships["patent_id"].isin(patents["patent_id"].astype(str))
]
relationships = relationships[
    relationships["inventor_id"].isin(inventors["inventor_id"].astype(str))
]
relationships = relationships[
    relationships["company_id"].isin(companies["company_id"].astype(str))
]

log(f"  Relationships before filter : {before:,}")
log(f"  Relationships after filter  : {len(relationships):,}")
save_report("relationships", relationships)


# ── SAVE ──────────────────────────────────────────────────────────────────────
log("Saving cleaned datasets...")

patents.to_csv("data/clean_patents.csv",           index=False)
inventors.to_csv("data/clean_inventors.csv",       index=False)
companies.to_csv("data/clean_companies.csv",       index=False)
relationships.to_csv("data/clean_relationships.csv", index=False)

log("Summary of clean files:")
for fname in ["clean_patents.csv", "clean_inventors.csv",
              "clean_companies.csv", "clean_relationships.csv"]:
    path = f"data/{fname}"
    if os.path.exists(path):
        size = os.path.getsize(path) / 1024
        rows = pd.read_csv(path).shape[0]
        log(f"  {fname:35s} {rows:6,} rows  {size:8.1f} KB")

log("ALL CLEANING COMPLETE ✔")
log("Next step: python scripts/03_load_database.py")