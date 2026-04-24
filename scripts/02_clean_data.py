import pandas as pd
import os

os.makedirs("reports", exist_ok=True)

print("Loading raw data...")
patents_df = pd.read_csv("data/raw_patents.csv")
inventors_df = pd.read_csv("data/raw_inventors.csv")
companies_df = pd.read_csv("data/raw_companies.csv")
relationships_df = pd.read_csv("data/raw_relationships.csv")

# CLEAN PATENTS
print("\nCleaning patents...")
print(f"  Before: {len(patents_df)} rows")
patents_df = patents_df.drop_duplicates(subset="patent_id")
patents_df = patents_df.dropna(subset=["patent_id", "title", "filing_date"])
patents_df["abstract"] = patents_df["abstract"].fillna("No abstract available")
patents_df["title"] = patents_df["title"].str.strip().str.title()
patents_df["abstract"] = patents_df["abstract"].str.strip()
patents_df["year"] = pd.to_numeric(patents_df["year"], errors="coerce")
patents_df = patents_df.dropna(subset=["year"])
patents_df["year"] = patents_df["year"].astype(int)
patents_df["filing_date"] = pd.to_datetime(patents_df["filing_date"], errors="coerce").dt.strftime("%Y-%m-%d")
patents_df = patents_df.dropna(subset=["filing_date"])
print(f"  After:  {len(patents_df)} rows")

# CLEAN INVENTORS
print("\nCleaning inventors...")
print(f"  Before: {len(inventors_df)} rows")
inventors_df = inventors_df.drop_duplicates(subset="inventor_id")
inventors_df = inventors_df.dropna(subset=["inventor_id", "name"])
inventors_df["name"] = inventors_df["name"].str.strip().str.title()
inventors_df["country"] = inventors_df["country"].str.strip().str.upper()
inventors_df["country"] = inventors_df["country"].fillna("Unknown")
print(f"  After:  {len(inventors_df)} rows")

# CLEAN COMPANIES
print("\nCleaning companies...")
print(f"  Before: {len(companies_df)} rows")
companies_df = companies_df.drop_duplicates(subset="company_id")
companies_df = companies_df.dropna(subset=["company_id", "name"])
companies_df["name"] = companies_df["name"].str.strip().str.title()
print(f"  After:  {len(companies_df)} rows")

# CLEAN RELATIONSHIPS
print("\nCleaning relationships...")
print(f"  Before: {len(relationships_df)} rows")
relationships_df = relationships_df.drop_duplicates()
relationships_df = relationships_df.dropna()
relationships_df = relationships_df[relationships_df["patent_id"].isin(patents_df["patent_id"])]
relationships_df = relationships_df[relationships_df["inventor_id"].isin(inventors_df["inventor_id"])]
relationships_df = relationships_df[relationships_df["company_id"].isin(companies_df["company_id"])]
print(f"  After:  {len(relationships_df)} rows")

# SAVE CLEAN FILES
print("\nSaving clean data...")
patents_df.to_csv("data/clean_patents.csv", index=False)
inventors_df.to_csv("data/clean_inventors.csv", index=False)
companies_df.to_csv("data/clean_companies.csv", index=False)
relationships_df.to_csv("data/clean_relationships.csv", index=False)

print("  clean_patents.csv done")
print("  clean_inventors.csv done")
print("  clean_companies.csv done")
print("  clean_relationships.csv done")
print("\nData cleaning complete!")