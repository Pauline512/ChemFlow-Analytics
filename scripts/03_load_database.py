"""
03_load_database.py
===================
Loads cleaned CSV data into a SQLite database.
Also executes schema.sql to ensure tables are properly defined.

Reads from:  data/clean_*.csv
             database/schema.sql
Writes to:   database/patents.db
"""

import sqlite3
import pandas as pd
import os

# ── SETUP ─────────────────────────────────────────────────────────────────────
DB_PATH     = "database/patents.db"
SCHEMA_PATH = "database/schema.sql"
os.makedirs("database", exist_ok=True)

def log(msg):
    print(f"[INFO] {msg}")

# ── CONNECT ───────────────────────────────────────────────────────────────────
log(f"Connecting to database: {DB_PATH}")
conn   = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Enable foreign key enforcement
cursor.execute("PRAGMA foreign_keys = ON")

# ── CREATE TABLES FROM SCHEMA ─────────────────────────────────────────────────
log("Creating tables from schema.sql ...")

# Drop existing tables so we always start fresh
cursor.executescript("""
    DROP TABLE IF EXISTS relationships;
    DROP TABLE IF EXISTS patents;
    DROP TABLE IF EXISTS inventors;
    DROP TABLE IF EXISTS companies;
""")

cursor.executescript("""
-- Patents table
CREATE TABLE IF NOT EXISTS patents (
    patent_id   TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    abstract    TEXT,
    filing_date TEXT,
    year        INTEGER
);

-- Inventors table
CREATE TABLE IF NOT EXISTS inventors (
    inventor_id TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    country     TEXT
);

-- Companies (Assignees) table
CREATE TABLE IF NOT EXISTS companies (
    company_id  TEXT PRIMARY KEY,
    name        TEXT NOT NULL
);

-- Relationships table
CREATE TABLE IF NOT EXISTS relationships (
    patent_id   TEXT,
    inventor_id TEXT,
    company_id  TEXT,
    FOREIGN KEY (patent_id)   REFERENCES patents(patent_id),
    FOREIGN KEY (inventor_id) REFERENCES inventors(inventor_id),
    FOREIGN KEY (company_id)  REFERENCES companies(company_id)
);
""")

conn.commit()
log("Tables created successfully")

# ── LOAD CLEAN CSVs ───────────────────────────────────────────────────────────
log("Loading clean CSV files...")

patents_df       = pd.read_csv("data/clean_patents.csv")
inventors_df     = pd.read_csv("data/clean_inventors.csv")
companies_df     = pd.read_csv("data/clean_companies.csv")
relationships_df = pd.read_csv("data/clean_relationships.csv")

log(f"  clean_patents       : {len(patents_df):,} rows")
log(f"  clean_inventors     : {len(inventors_df):,} rows")
log(f"  clean_companies     : {len(companies_df):,} rows")
log(f"  clean_relationships : {len(relationships_df):,} rows")

# ── ENSURE CORRECT COLUMN ORDER & TYPES ───────────────────────────────────────
# Patents
patents_df = patents_df[["patent_id", "title", "abstract", "filing_date", "year"]]
patents_df["patent_id"]   = patents_df["patent_id"].astype(str)
patents_df["year"]        = pd.to_numeric(patents_df["year"], errors="coerce")
patents_df["filing_date"] = patents_df["filing_date"].astype(str)

# Inventors
inventors_df = inventors_df[["inventor_id", "name", "country"]]
inventors_df["inventor_id"] = inventors_df["inventor_id"].astype(str)

# Companies
companies_df = companies_df[["company_id", "name"]]
companies_df["company_id"] = companies_df["company_id"].astype(str)

# Relationships
relationships_df = relationships_df[["patent_id", "inventor_id", "company_id"]]
relationships_df["patent_id"]   = relationships_df["patent_id"].astype(str)
relationships_df["inventor_id"] = relationships_df["inventor_id"].astype(str)
relationships_df["company_id"]  = relationships_df["company_id"].astype(str)

# ── INSERT INTO DATABASE ───────────────────────────────────────────────────────
log("Inserting data into database...")

patents_df.to_sql("patents", conn, if_exists="replace", index=False)
log(f"  patents       : {len(patents_df):,} rows inserted")

inventors_df.to_sql("inventors", conn, if_exists="replace", index=False)
log(f"  inventors     : {len(inventors_df):,} rows inserted")

companies_df.to_sql("companies", conn, if_exists="replace", index=False)
log(f"  companies     : {len(companies_df):,} rows inserted")

relationships_df.to_sql("relationships", conn, if_exists="replace", index=False)
log(f"  relationships : {len(relationships_df):,} rows inserted")

conn.commit()

# ── VERIFY ────────────────────────────────────────────────────────────────────
log("Verifying row counts in database...")
for table in ["patents", "inventors", "companies", "relationships"]:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    log(f"  {table:20s} : {count:,} rows")

# Quick sanity check — preview first row of each table
log("Sample rows:")
for table in ["patents", "inventors", "companies", "relationships"]:
    cursor.execute(f"SELECT * FROM {table} LIMIT 1")
    row = cursor.fetchone()
    log(f"  {table}: {row}")

conn.close()

log("=" * 50)
log(f"Database saved → {DB_PATH}")
log("Next step: python scripts/04_queries.py")
log("=" * 50)