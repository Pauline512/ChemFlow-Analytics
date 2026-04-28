"""
01_download_data.py
===================
Downloads real USPTO patent data from PatentsView (S3 direct links).
Filters to a specific technology topic using CPC classification codes.

Topic choices:
  - "chemistry"  → CPC section C  (C01–C40: chemistry, metallurgy)
  - "lighting"   → CPC class  F21 (lighting devices and systems)

Outputs (saved to data/):
  raw_patents.csv, raw_inventors.csv,
  raw_companies.csv, raw_relationships.csv
"""

import requests
import pandas as pd
import zipfile
import io
import os

# ── Configuration ──────────────────────────────────────────────────────────────

TOPIC = "chemistry"   # change to "lighting" if preferred

TOPIC_CPC = {
    "chemistry": "C",    # All of CPC section C (chemistry & metallurgy)
    "lighting":  "F21",  # CPC class F21 (lighting)
}

CPC_PREFIX = TOPIC_CPC[TOPIC]
MAX_PATENTS = 5000   # max patent IDs to keep after CPC filter

os.makedirs("data", exist_ok=True)

# ── Direct S3 download URLs ────────────────────────────────────────────────────
S3 = "https://s3.amazonaws.com/data.patentsview.org/download/"

FILES = {
    "cpc":             S3 + "g_cpc_current.tsv.zip",
    "patent":          S3 + "g_patent.tsv.zip",
    "patent_abstract": S3 + "g_patent_abstract.tsv.zip",
    "inventor":        S3 + "g_inventor_disambiguated.tsv.zip",
    "assignee":        S3 + "g_assignee_disambiguated.tsv.zip",
    "patent_inventor": S3 + "g_patent_inventor.tsv.zip",
    "patent_assignee": S3 + "g_patent_assignee.tsv.zip",
    "location":        S3 + "g_location_disambiguated.tsv.zip",
}


# ── Helper: stream zip → DataFrame ────────────────────────────────────────────
def download_tsv(label, url, nrows=None, usecols=None):
    """Stream a .tsv.zip from URL, decompress in memory, return DataFrame."""
    print(f"\n  [{label}]")
    print(f"  Fetching: {url}")
    r = requests.get(url, stream=True, timeout=300)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} — could not reach {url}")

    print("  Downloading...", end=" ", flush=True)
    chunks = []
    total = 0
    for chunk in r.iter_content(chunk_size=1024 * 512):
        chunks.append(chunk)
        total += len(chunk)
        print(f"\r  Downloaded {total / 1e6:.1f} MB", end="", flush=True)
    content = b"".join(chunks)
    print(f" — done ({total / 1e6:.1f} MB)")

    with zipfile.ZipFile(io.BytesIO(content)) as z:
        tsv_name = next(f for f in z.namelist() if f.endswith(".tsv"))
        print(f"  Reading {tsv_name} ...", end=" ", flush=True)
        with z.open(tsv_name) as f:
            df = pd.read_csv(
                f, sep="\t", low_memory=False,
                nrows=nrows, usecols=usecols,
                on_bad_lines="skip"
            )
    print(f"{len(df):,} rows | cols: {list(df.columns)}")
    return df


def filter_by_patent_ids(df, id_set):
    """Keep only rows whose patent_id is in id_set."""
    col = next((c for c in df.columns if "patent_id" in c.lower()), None)
    if col:
        return df[df[col].astype(str).isin(id_set)].copy()
    return df


# ── STEP 1: CPC filter — get patent IDs for chosen topic ──────────────────────
print("=" * 62)
print(f"  PatentsView Patent Pipeline — topic: {TOPIC.upper()}")
print(f"  CPC filter prefix: '{CPC_PREFIX}'")
print("=" * 62)

cpc_df = download_tsv("CPC classifications", FILES["cpc"])

# Detect CPC code column
cpc_code_col = next(
    (c for c in cpc_df.columns if "cpc" in c.lower() and
     any(x in c.lower() for x in ["id", "code", "group", "subgroup"])),
    cpc_df.columns[1]
)
pat_col_cpc = next(c for c in cpc_df.columns if "patent_id" in c.lower())

print(f"\n  CPC code column: '{cpc_code_col}'")
print(f"  Sample values:   {cpc_df[cpc_code_col].dropna().head(5).tolist()}")

mask = cpc_df[cpc_code_col].astype(str).str.startswith(CPC_PREFIX)
topic_cpc = cpc_df[mask]
print(f"  Rows matching '{CPC_PREFIX}*': {len(topic_cpc):,}")

topic_patent_ids = topic_cpc[pat_col_cpc].dropna().unique()[:MAX_PATENTS]
topic_patent_ids_set = set(topic_patent_ids.astype(str))
print(f"  Unique patent IDs selected:  {len(topic_patent_ids_set):,}")


# ── STEP 2: Download and filter remaining tables ───────────────────────────────

patent_raw   = download_tsv("g_patent",                FILES["patent"])
patent_raw   = filter_by_patent_ids(patent_raw, topic_patent_ids_set)
print(f"  After topic filter: {len(patent_raw):,} patents")

abstract_raw = download_tsv("g_patent_abstract",       FILES["patent_abstract"])
abstract_raw = filter_by_patent_ids(abstract_raw, topic_patent_ids_set)

inventor_raw = download_tsv("g_inventor_disambiguated", FILES["inventor"])
assignee_raw = download_tsv("g_assignee_disambiguated", FILES["assignee"])

pi_raw       = download_tsv("g_patent_inventor",        FILES["patent_inventor"])
pi_raw       = filter_by_patent_ids(pi_raw, topic_patent_ids_set)

pa_raw       = download_tsv("g_patent_assignee",        FILES["patent_assignee"])
pa_raw       = filter_by_patent_ids(pa_raw, topic_patent_ids_set)

location_raw = download_tsv("g_location_disambiguated", FILES["location"])


# ── STEP 3: raw_patents.csv ────────────────────────────────────────────────────
print("\n" + "─" * 62)
print("Building raw_patents.csv ...")

pid   = next(c for c in patent_raw.columns if "patent_id" in c.lower())
ptitl = next(c for c in patent_raw.columns if "title"     in c.lower())
pdate = next(c for c in patent_raw.columns if "date"      in c.lower())

patents_out = patent_raw[[pid, ptitl, pdate]].copy()
patents_out.columns = ["patent_id", "title", "filing_date"]
patents_out["filing_date"] = pd.to_datetime(patents_out["filing_date"], errors="coerce")
patents_out["year"]        = patents_out["filing_date"].dt.year
patents_out["filing_date"] = patents_out["filing_date"].dt.strftime("%Y-%m-%d")

ab_pid = next((c for c in abstract_raw.columns if "patent_id" in c.lower()), None)
ab_txt = next((c for c in abstract_raw.columns if "abstract"  in c.lower()), None)
if ab_pid and ab_txt:
    abs_clean = abstract_raw[[ab_pid, ab_txt]].rename(
        columns={ab_pid: "patent_id", ab_txt: "abstract"})
    patents_out = patents_out.merge(abs_clean, on="patent_id", how="left")
else:
    patents_out["abstract"] = ""

patents_out.to_csv("data/raw_patents.csv", index=False)
print(f"  → data/raw_patents.csv        ({len(patents_out):,} rows)")


# ── STEP 4: raw_inventors.csv ──────────────────────────────────────────────────
print("\nBuilding raw_inventors.csv ...")

inv_id  = next(c for c in inventor_raw.columns if "inventor_id"  in c.lower())
inv_fn  = next((c for c in inventor_raw.columns if "first"       in c.lower()), None)
inv_ln  = next((c for c in inventor_raw.columns if "last"        in c.lower()), None)
inv_loc = next((c for c in inventor_raw.columns if "location_id" in c.lower()), None)

inventors_out = pd.DataFrame()
inventors_out["inventor_id"] = inventor_raw[inv_id]
inventors_out["name"] = (
    (inventor_raw[inv_fn].fillna("") + " " + inventor_raw[inv_ln].fillna("")).str.strip()
    if inv_fn and inv_ln else inventor_raw[inv_id]
)

loc_id_col  = next((c for c in location_raw.columns if "location_id" in c.lower()), None)
loc_cty_col = next((c for c in location_raw.columns if "country"     in c.lower()), None)

if inv_loc and loc_id_col and loc_cty_col:
    loc_map = location_raw[[loc_id_col, loc_cty_col]].rename(
        columns={loc_id_col: "loc_id", loc_cty_col: "country"})
    inventors_out["loc_id"] = inventor_raw[inv_loc].values
    inventors_out = inventors_out.merge(loc_map, on="loc_id", how="left")
    inventors_out.drop(columns=["loc_id"], inplace=True)
else:
    inventors_out["country"] = "Unknown"

inventors_out.to_csv("data/raw_inventors.csv", index=False)
print(f"  → data/raw_inventors.csv      ({len(inventors_out):,} rows)")


# ── STEP 5: raw_companies.csv ──────────────────────────────────────────────────
print("\nBuilding raw_companies.csv ...")

asg_id  = next(c for c in assignee_raw.columns if "assignee_id"   in c.lower())
asg_org = next((c for c in assignee_raw.columns if "organization" in c.lower()), None)
asg_nm  = next((c for c in assignee_raw.columns if "name"         in c.lower()), None)

companies_out = pd.DataFrame()
companies_out["company_id"] = assignee_raw[asg_id]
companies_out["name"] = (
    assignee_raw[asg_org] if asg_org else
    assignee_raw[asg_nm]  if asg_nm  else
    assignee_raw[asg_id]
)
companies_out = companies_out.dropna(subset=["name"])
companies_out.to_csv("data/raw_companies.csv", index=False)
print(f"  → data/raw_companies.csv      ({len(companies_out):,} rows)")


# ── STEP 6: raw_relationships.csv ─────────────────────────────────────────────
print("\nBuilding raw_relationships.csv ...")

pi_pat = next(c for c in pi_raw.columns if "patent_id"   in c.lower())
pi_inv = next(c for c in pi_raw.columns if "inventor_id" in c.lower())
inv_links = pi_raw[[pi_pat, pi_inv]].rename(
    columns={pi_pat: "patent_id", pi_inv: "inventor_id"})

pa_pat = next(c for c in pa_raw.columns if "patent_id"   in c.lower())
pa_asg = next(c for c in pa_raw.columns if "assignee_id" in c.lower())
asg_links = pa_raw[[pa_pat, pa_asg]].rename(
    columns={pa_pat: "patent_id", pa_asg: "company_id"})

relationships_out = inv_links.merge(asg_links, on="patent_id", how="inner")
relationships_out = relationships_out.dropna()
relationships_out.to_csv("data/raw_relationships.csv", index=False)
print(f"  → data/raw_relationships.csv  ({len(relationships_out):,} rows)")


# ── Summary ────────────────────────────────────────────────────────────────────
print("\n" + "=" * 62)
print(f"  Done! Topic: {TOPIC.upper()}  |  CPC prefix: {CPC_PREFIX}*")
print("  Raw files saved to data/:")
for f in ["raw_patents.csv", "raw_inventors.csv",
          "raw_companies.csv", "raw_relationships.csv"]:
    path = f"data/{f}"
    if os.path.exists(path):
        kb = os.path.getsize(path) / 1024
        print(f"    {f:35s} {kb:8.1f} KB")
print("\n  Next step: python scripts/02_clean_data.py")
print("=" * 62)