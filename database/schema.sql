import sqlite3
import pandas as pd
import os

DB_PATH = "database/patents.db"
os.makedirs("database", exist_ok=True)

print("Connecting to database...")
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

print("Creating tables from schema...")
with open("database/schema.sql", "r") as f:
    schema = f.read()
cursor.executescript(schema)
conn.commit()

print("Loading clean data...")
patents_df = pd.read_csv("data/clean_patents.csv")
inventors_df = pd.read_csv("data/clean_inventors.csv")
companies_df = pd.read_csv("data/clean_companies.csv")
relationships_df = pd.read_csv("data/clean_relationships.csv")

print("Inserting data into database...")
patents_df.to_sql("patents", conn, if_exists="replace", index=False)
print(f"  patents table: {len(patents_df)} rows inserted")

inventors_df.to_sql("inventors", conn, if_exists="replace", index=False)
print(f"  inventors table: {len(inventors_df)} rows inserted")

companies_df.to_sql("companies", conn, if_exists="replace", index=False)
print(f"  companies table: {len(companies_df)} rows inserted")

relationships_df.to_sql("relationships", conn, if_exists="replace", index=False)
print(f"  relationships table: {len(relationships_df)} rows inserted")

print("\nVerifying tables...")
tables = ["patents", "inventors", "companies", "relationships"]
for table in tables:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    print(f"  {table}: {count} rows")

conn.close()
print("\nDatabase created successfully at database/patents.db")