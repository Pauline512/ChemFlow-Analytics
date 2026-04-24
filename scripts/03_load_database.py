import sqlite3
import pandas as pd
import os

DB_PATH = "database/patents.db"
os.makedirs("database", exist_ok=True)

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS patents (
    patent_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    abstract TEXT,
    filing_date TEXT,
    year INTEGER
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS inventors (
    inventor_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    country TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS companies (
    company_id TEXT PRIMARY KEY,
    name TEXT NOT NULL
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS relationships (
    patent_id TEXT,
    inventor_id TEXT,
    company_id TEXT,
    FOREIGN KEY (patent_id) REFERENCES patents(patent_id),
    FOREIGN KEY (inventor_id) REFERENCES inventors(inventor_id),
    FOREIGN KEY (company_id) REFERENCES companies(company_id)
)
""")

conn.commit()
print("Tables created successfully")

patents_df = pd.read_csv("data/clean_patents.csv")
inventors_df = pd.read_csv("data/clean_inventors.csv")
companies_df = pd.read_csv("data/clean_companies.csv")
relationships_df = pd.read_csv("data/clean_relationships.csv")

patents_df.to_sql("patents", conn, if_exists="replace", index=False)
print(f"patents table: {len(patents_df)} rows inserted")

inventors_df.to_sql("inventors", conn, if_exists="replace", index=False)
print(f"inventors table: {len(inventors_df)} rows inserted")

companies_df.to_sql("companies", conn, if_exists="replace", index=False)
print(f"companies table: {len(companies_df)} rows inserted")

relationships_df.to_sql("relationships", conn, if_exists="replace", index=False)
print(f"relationships table: {len(relationships_df)} rows inserted")

print("\nVerifying tables...")
for table in ["patents", "inventors", "companies", "relationships"]:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    print(f"  {table}: {count} rows")

conn.close()
print("\nDone! Database saved at database/patents.db")