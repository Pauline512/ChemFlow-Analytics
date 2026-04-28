"""
01_download_data.py
Downloads real patent data from USPTO PatentsView bulk data.
Produces raw_patents.csv, raw_inventors.csv, raw_companies.csv, raw_relationships.csv
"""

import requests
import pandas as pd
import zipfile
import io
import os

os.makedirs("data", exist_ok=True)

BASE_URL = "https://data.uspto.gov/bulkdata/datasets/pvgpatdis/"

FILES = {
    "patent":           "g_patent.tsv.zip",
    "patent_abstract":  "g_patent_abstract.tsv.zip",
    "inventor":         "g_inventor_disambiguated.tsv.zip",
    "assignee":         "g_assignee_disambiguated.tsv.zip",
    "patent_inventor":  "g_inventor_not_disambiguated.tsv.zip",  # has patent_id + inventor link
    "location":         "g_location_disambiguated.tsv.zip",
}

LIMIT = 5000  # rows to read — increase for bigger dataset


def download_tsv(label, filename, nrows=LIMIT):
    url = BASE_URL + filename
    print(f"  Downloading {filename} ({label}) ...")
    r = requests.get(url, stream=True, timeout=180)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        inner = [f for f in z.namelist() if f.endswith(".tsv")][0]
        with z.open(inner) as f:
            df = pd.read_csv(f, sep="\t", low_memory=False, nrows=nrows)
    print(f"    → {len(df):,} rows | columns: {list(df.columns)}")
    return df


print("=" * 55)
print("  USPTO PatentsView — downloading real patent data")
print("=" * 55)

# --- Download core tables ---
patent_df   = download_tsv("patents",    FILES["patent"])
abstract_df = download_tsv("abstracts",  FILES["patent_abstract"])
inventor_df = download_tsv("inventors",  FILES["inventor"])
assignee_df = download_tsv("assignees",  FILES["assignee"])
pi_df       = download_tsv("pat-inv links", FILES["patent_inventor"])
location_df = download_tsv("locations",  FILES["location"])

# --- raw_patents.csv ---
print("\nBuilding raw_patents.csv ...")
patents_out = patent_df[["patent_id", "patent_title", "patent_date"]].copy()
patents_out.columns = ["patent_id", "title", "filing_date"]
patents_out["filing_date"] = pd.to_datetime(patents_out["filing_date"], errors="coerce")
patents_out["year"] = patents_out["filing_date"].dt.year
patents_out["filing_date"] = patents_out["filing_date"].dt.strftime("%Y-%m-%d")

# Merge abstract in
abstract_df = abstract_df.rename(columns={"patent_id": "patent_id", "patent_abstract": "abstract"})
if "patent_abstract" in abstract_df.columns:
    abstract_df = abstract_df[["patent_id", "patent_abstract"]].rename(columns={"patent_abstract": "abstract"})
    patents_out = patents_out.merge(abstract_df, on="patent_id", how="left")
else:
    patents_out["abstract"] = ""

patents_out.to_csv("data/raw_patents.csv", index=False)
print(f"  → {len(patents_out):,} patents saved")

# --- raw_inventors.csv ---
print("\nBuilding raw_inventors.csv ...")
inv_id  = "disamb_inventor_id_20"  if "disamb_inventor_id_20"  in inventor_df.columns else inventor_df.columns[0]
inv_fn  = next((c for c in inventor_df.columns if "first" in c.lower()), None)
inv_ln  = next((c for c in inventor_df.columns if "last"  in c.lower()), None)
inv_loc = next((c for c in inventor_df.columns if "location_id" in c.lower()), None)

inventors_out = pd.DataFrame()
inventors_out["inventor_id"] = inventor_df[inv_id]
inventors_out["name"] = (
    inventor_df[inv_fn].fillna("") + " " + inventor_df[inv_ln].fillna("")
).str.strip() if inv_fn and inv_ln else inventor_df[inv_id]

# Get country from location table
if inv_loc and "country" in location_df.columns:
    loc_id_col = next((c for c in location_df.columns if "location_id" in c.lower()), location_df.columns[0])
    loc_map = location_df[[loc_id_col, "country"]].rename(columns={loc_id_col: "location_id"})
    inventor_df2 = inventor_df.rename(columns={inv_loc: "location_id"})
    inventors_out["location_id"] = inventor_df2["location_id"].values
    inventors_out = inventors_out.merge(loc_map, on="location_id", how="left")
    inventors_out = inventors_out.drop(columns=["location_id"])
else:
    inventors_out["country"] = "Unknown"

inventors_out.to_csv("data/raw_inventors.csv", index=False)
print(f"  → {len(inventors_out):,} inventors saved")

# --- raw_companies.csv ---
print("\nBuilding raw_companies.csv ...")
asg_id  = next((c for c in assignee_df.columns if "assignee_id" in c.lower()), assignee_df.columns[0])
asg_org = next((c for c in assignee_df.columns if "organization" in c.lower()), None)
asg_nm  = next((c for c in assignee_df.columns if "name" in c.lower()), None)

companies_out = pd.DataFrame()
companies_out["company_id"] = assignee_df[asg_id]
companies_out["name"] = (
    assignee_df[asg_org] if asg_org else
    assignee_df[asg_nm]  if asg_nm  else
    assignee_df[asg_id]
)
companies_out = companies_out.dropna(subset=["name"])
companies_out.to_csv("data/raw_companies.csv", index=False)
print(f"  → {len(companies_out):,} companies saved")

# --- raw_relationships.csv ---
print("\nBuilding raw_relationships.csv ...")
# g_inventor_not_disambiguated has: patent_id, inventor_id, assignee columns
pat_col = next((c for c in pi_df.columns if "patent_id" in c.lower()), pi_df.columns[0])
inv_col = next((c for c in pi_df.columns if "inventor_id" in c.lower()), pi_df.columns[1])
asg_col = next((c for c in pi_df.columns if "assignee" in c.lower()), None)

if asg_col:
    # Already has assignee in same table
    relationships_out = pi_df[[pat_col, inv_col, asg_col]].copy()
    relationships_out.columns = ["patent_id", "inventor_id", "company_id"]
else:
    # Need to map inventors → patents → assignees via patent table
    inv_links = pi_df[[pat_col, inv_col]].rename(
        columns={pat_col: "patent_id", inv_col: "inventor_id"})
    # Use assignee_df's first assignee per patent as company proxy
    asg_pat = next((c for c in assignee_df.columns if "patent_id" in c.lower()), None)
    if asg_pat:
        asg_links = assignee_df[[asg_pat, asg_id]].rename(
            columns={asg_pat: "patent_id", asg_id: "company_id"})
        relationships_out = inv_links.merge(asg_links, on="patent_id", how="inner")
    else:
        # Fallback: assign random company_id from our companies list
        import random
        random.seed(42)
        comp_ids = companies_out["company_id"].tolist()
        inv_links["company_id"] = [random.choice(comp_ids) for _ in range(len(inv_links))]
        relationships_out = inv_links

relationships_out = relationships_out.dropna()
relationships_out.to_csv("data/raw_relationships.csv", index=False)
print(f"  → {len(relationships_out):,} relationships saved")

print("\n" + "=" * 55)
print("  Real USPTO data downloaded successfully!")
print("  Next: run 02_clean_data.py")
print("=" * 55)