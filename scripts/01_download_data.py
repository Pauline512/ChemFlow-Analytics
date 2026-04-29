"""
01_download_data.py
===================
Downloads real USPTO patent data from PatentsView (S3 direct links).
Filters to Chemistry topic using CPC section 'C'.
Uses chunked reading to stay memory-safe with large files.

Outputs saved to data/:
    raw_patents.csv        — patent_id, title, filing_date, abstract, year
    raw_inventors.csv      — inventor_id, name, country
    raw_companies.csv      — company_id, name
    raw_relationships.csv  — patent_id, inventor_id, company_id
"""

import pandas as pd
import requests
import zipfile
import io
import os
import time

# ── CONFIG ────────────────────────────────────────────────────────────────────
TOPIC_NAME  = "CHEMISTRY"
CPC_FILTER  = "C"           # CPC section C = chemistry & metallurgy
MAX_PATENTS = 5000           # cap so files stay manageable
CHUNK_SIZE  = 50_000
BLOCK_SIZE  = 1024 * 1024   # 1 MB download blocks

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

S3 = "https://s3.amazonaws.com/data.patentsview.org/download/"

FILES = {
    "cpc":             S3 + "g_cpc_current.tsv.zip",
    "patent":          S3 + "g_patent.tsv.zip",
    "patent_abstract": S3 + "g_patent_abstract.tsv.zip",
    "inventor":        S3 + "g_inventor_disambiguated.tsv.zip",
    "assignee":        S3 + "g_assignee_disambiguated.tsv.zip",
    # ✅ FIXED URLs — these are the correct filenames on S3
    "patent_inventor": S3 + "g_inventor_not_disambiguated.tsv.zip",
    "patent_assignee": S3 + "g_assignee_not_disambiguated.tsv.zip",
    "location":        S3 + "g_location_not_disambiguated.tsv.zip",
}


# ── HELPER: download with live progress ───────────────────────────────────────
def download_file(url):
    print(f"  Connecting to:\n  {url}")
    r = requests.get(url, stream=True, timeout=300)
    r.raise_for_status()

    total = int(r.headers.get("content-length", 0))
    downloaded = 0
    data = bytearray()
    start = time.time()

    for block in r.iter_content(BLOCK_SIZE):
        if block:
            data.extend(block)
            downloaded += len(block)
            if total > 0:
                pct = (downloaded / total) * 100
                mb  = downloaded / 1e6
                tmb = total / 1e6
                print(f"\r  Downloading: {mb:.1f}/{tmb:.1f} MB ({pct:.1f}%)",
                      end="", flush=True)

    print(f"\n  Done in {time.time() - start:.1f}s")
    return io.BytesIO(data)


# ── HELPER: read TSV in chunks with optional filter ───────────────────────────
def read_tsv_chunks(name, url, usecols=None,
                    filter_col=None, filter_values=None,
                    filter_prefix=None, max_rows=None):
    print("\n" + "=" * 60)
    print(f"  [{name}]")

    zip_data  = download_file(url)
    z         = zipfile.ZipFile(zip_data)
    file_name = z.namelist()[0]
    print(f"  Reading: {file_name} in chunks...")

    kept_chunks = []
    total_rows  = 0
    kept_rows   = 0

    with z.open(file_name) as f:
        reader = pd.read_csv(f, sep="\t", usecols=usecols,
                             low_memory=False, chunksize=CHUNK_SIZE,
                             on_bad_lines="skip")
        for i, chunk in enumerate(reader, 1):
            total_rows += len(chunk)

            if filter_col and filter_prefix:
                chunk = chunk[chunk[filter_col].astype(str)
                              .str.startswith(filter_prefix)]
            elif filter_col and filter_values is not None:
                chunk = chunk[chunk[filter_col].astype(str)
                              .isin(filter_values)]

            kept_rows += len(chunk)
            kept_chunks.append(chunk)

            print(f"\r  Chunk {i:>4}: read {total_rows:,} | kept {kept_rows:,}",
                  end="", flush=True)

            if max_rows and kept_rows >= max_rows:
                break

    print(f"\n  Finished — total read: {total_rows:,} | kept: {kept_rows:,}")
    df = pd.concat(kept_chunks, ignore_index=True) if kept_chunks else pd.DataFrame()
    return df.head(max_rows) if max_rows else df


# ══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print(f"  PatentsView Pipeline  |  Topic: {TOPIC_NAME}")
print(f"  CPC Filter: section '{CPC_FILTER}' (chemistry & metallurgy)")
print("=" * 60)

# ── STEP 1: CPC — get chemistry patent IDs ────────────────────────────────────
cpc_df = read_tsv_chunks(
    "CPC classifications", FILES["cpc"],
    usecols=["patent_id", "cpc_section"],
    filter_col="cpc_section",
    filter_prefix=CPC_FILTER
)

chem_ids = set(cpc_df["patent_id"].dropna().astype(str).unique()[:MAX_PATENTS])
print(f"\n  Chemistry patent IDs selected: {len(chem_ids):,}")
del cpc_df  # free memory


# ── STEP 2: Patents ───────────────────────────────────────────────────────────
patent_df = read_tsv_chunks(
    "Patents", FILES["patent"],
    usecols=["patent_id", "patent_title", "patent_date"],
    filter_col="patent_id", filter_values=chem_ids
)


# ── STEP 3: Abstracts ─────────────────────────────────────────────────────────
abstract_df = read_tsv_chunks(
    "Abstracts", FILES["patent_abstract"],
    filter_col="patent_id", filter_values=chem_ids
)
ab_txt_col = next((c for c in abstract_df.columns
                   if "abstract" in c.lower()), None)


# ── STEP 4: Patent–Inventor links (not-disambiguated has patent_id + inventor_id)
pi_df = read_tsv_chunks(
    "Patent-Inventor links", FILES["patent_inventor"],
    filter_col="patent_id", filter_values=chem_ids
)
print(f"  Columns in patent_inventor: {list(pi_df.columns)}")

# Get inventor IDs linked to our chemistry patents
inv_id_col_pi = next((c for c in pi_df.columns if "inventor_id" in c.lower()), None)
chem_inv_ids  = set(pi_df[inv_id_col_pi].dropna().astype(str).unique()) if inv_id_col_pi else set()
print(f"  Chemistry-linked inventor IDs: {len(chem_inv_ids):,}")


# ── STEP 5: Patent–Assignee links ─────────────────────────────────────────────
pa_df = read_tsv_chunks(
    "Patent-Assignee links", FILES["patent_assignee"],
    filter_col="patent_id", filter_values=chem_ids
)
print(f"  Columns in patent_assignee: {list(pa_df.columns)}")

# Get assignee IDs linked to our chemistry patents
asg_id_col_pa = next((c for c in pa_df.columns if "assignee_id" in c.lower()), None)
chem_asg_ids  = set(pa_df[asg_id_col_pa].dropna().astype(str).unique()) if asg_id_col_pa else set()
print(f"  Chemistry-linked assignee IDs: {len(chem_asg_ids):,}")


# ── STEP 6: Inventors — filtered to only chemistry-linked inventors ───────────
inventor_df = read_tsv_chunks(
    "Inventors (filtered)", FILES["inventor"],
    filter_col="disamb_inventor_id_20" if "disamb_inventor_id_20" in ["disamb_inventor_id_20"] else None,
    filter_values=chem_inv_ids if chem_inv_ids else None
)
# Re-filter by inventor_id if we have chem_inv_ids
if chem_inv_ids:
    inv_id_col = next((c for c in inventor_df.columns if "inventor_id" in c.lower()), None)
    if inv_id_col:
        inventor_df = inventor_df[inventor_df[inv_id_col].astype(str).isin(chem_inv_ids)]
        print(f"  After inventor filter: {len(inventor_df):,} rows")


# ── STEP 7: Assignees — filtered to only chemistry-linked assignees ───────────
assignee_df = read_tsv_chunks(
    "Assignees (filtered)", FILES["assignee"],
    filter_col="assignee_id" if chem_asg_ids else None,
    filter_values=chem_asg_ids if chem_asg_ids else None
)
if chem_asg_ids:
    asg_id_col = next((c for c in assignee_df.columns if "assignee_id" in c.lower()), None)
    if asg_id_col:
        assignee_df = assignee_df[assignee_df[asg_id_col].astype(str).isin(chem_asg_ids)]
        print(f"  After assignee filter: {len(assignee_df):,} rows")


# ── STEP 8: Locations ─────────────────────────────────────────────────────────
location_df = read_tsv_chunks(
    "Locations", FILES["location"]
)
print(f"  Columns in location: {list(location_df.columns)}")


# ══════════════════════════════════════════════════════════════════════════════
#  BUILD OUTPUT FILES
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("  Building output CSV files...")
print("=" * 60)


# ── raw_patents.csv ───────────────────────────────────────────────────────────
print("\nBuilding raw_patents.csv ...")

patents_out = patent_df.rename(columns={
    "patent_title": "title",
    "patent_date":  "filing_date"
})[["patent_id", "title", "filing_date"]].copy()

patents_out["filing_date"] = pd.to_datetime(patents_out["filing_date"], errors="coerce")
patents_out["year"]        = patents_out["filing_date"].dt.year
patents_out["filing_date"] = patents_out["filing_date"].dt.strftime("%Y-%m-%d")

if ab_txt_col:
    ab_col = next((c for c in abstract_df.columns if "patent_id" in c.lower()), None)
    if ab_col:
        abs_clean = abstract_df[[ab_col, ab_txt_col]].rename(
            columns={ab_col: "patent_id", ab_txt_col: "abstract"})
        patents_out = patents_out.merge(abs_clean, on="patent_id", how="left")
    else:
        patents_out["abstract"] = ""
else:
    patents_out["abstract"] = ""

patents_out.to_csv(f"{DATA_DIR}/raw_patents.csv", index=False)
print(f"  → raw_patents.csv        ({len(patents_out):,} rows)")


# ── raw_inventors.csv ─────────────────────────────────────────────────────────
print("\nBuilding raw_inventors.csv ...")

inv_id  = next((c for c in inventor_df.columns if "inventor_id"  in c.lower()), inventor_df.columns[0])
inv_fn  = next((c for c in inventor_df.columns if "first"        in c.lower()), None)
inv_ln  = next((c for c in inventor_df.columns if "last"         in c.lower()), None)
inv_loc = next((c for c in inventor_df.columns if "location_id"  in c.lower()), None)

inventors_out = pd.DataFrame()
inventors_out["inventor_id"] = inventor_df[inv_id].values
inventors_out["name"] = (
    (inventor_df[inv_fn].fillna("") + " " +
     inventor_df[inv_ln].fillna("")).str.strip()
    if inv_fn and inv_ln else inventor_df[inv_id].values
)

# Attach country via location table
loc_id_col  = next((c for c in location_df.columns if "location_id" in c.lower()), None)
loc_cty_col = next((c for c in location_df.columns if "country"     in c.lower()), None)

if inv_loc and loc_id_col and loc_cty_col:
    loc_map = location_df[[loc_id_col, loc_cty_col]].rename(
        columns={loc_id_col: "loc_id", loc_cty_col: "country"})
    inventors_out["loc_id"] = inventor_df[inv_loc].values
    inventors_out = inventors_out.merge(loc_map, on="loc_id", how="left")
    inventors_out.drop(columns=["loc_id"], inplace=True)
else:
    inventors_out["country"] = "Unknown"

inventors_out.to_csv(f"{DATA_DIR}/raw_inventors.csv", index=False)
print(f"  → raw_inventors.csv      ({len(inventors_out):,} rows)")


# ── raw_companies.csv ─────────────────────────────────────────────────────────
print("\nBuilding raw_companies.csv ...")

asg_id  = next((c for c in assignee_df.columns if "assignee_id"   in c.lower()), assignee_df.columns[0])
asg_org = next((c for c in assignee_df.columns if "organization"  in c.lower()), None)
asg_nm  = next((c for c in assignee_df.columns if "name"          in c.lower()), None)

companies_out = pd.DataFrame()
companies_out["company_id"] = assignee_df[asg_id].values
companies_out["name"] = (
    assignee_df[asg_org].values if asg_org else
    assignee_df[asg_nm].values  if asg_nm  else
    assignee_df[asg_id].values
)
companies_out = companies_out.dropna(subset=["name"])
companies_out.to_csv(f"{DATA_DIR}/raw_companies.csv", index=False)
print(f"  → raw_companies.csv      ({len(companies_out):,} rows)")


# ── raw_relationships.csv ─────────────────────────────────────────────────────
print("\nBuilding raw_relationships.csv ...")

pi_pat = next((c for c in pi_df.columns if "patent_id"   in c.lower()), None)
pi_inv = next((c for c in pi_df.columns if "inventor_id" in c.lower()), None)

pa_pat = next((c for c in pa_df.columns if "patent_id"   in c.lower()), None)
pa_asg = next((c for c in pa_df.columns if "assignee_id" in c.lower()), None)

if pi_pat and pi_inv and pa_pat and pa_asg:
    inv_links = pi_df[[pi_pat, pi_inv]].rename(
        columns={pi_pat: "patent_id", pi_inv: "inventor_id"})
    asg_links = pa_df[[pa_pat, pa_asg]].rename(
        columns={pa_pat: "patent_id", pa_asg: "company_id"})
    relationships_out = inv_links.merge(asg_links, on="patent_id", how="inner")
    relationships_out = relationships_out.dropna()
else:
    print("  WARNING: Could not find expected columns in patent_inventor or patent_assignee")
    print(f"  patent_inventor cols: {list(pi_df.columns)}")
    print(f"  patent_assignee cols: {list(pa_df.columns)}")
    relationships_out = pd.DataFrame(columns=["patent_id", "inventor_id", "company_id"])

relationships_out.to_csv(f"{DATA_DIR}/raw_relationships.csv", index=False)
print(f"  → raw_relationships.csv  ({len(relationships_out):,} rows)")


# ── Summary ───────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print(f"  ALL DONE  |  Topic: {TOPIC_NAME}  |  CPC: {CPC_FILTER}*")
print("  Files saved to data/:")
for fname in ["raw_patents.csv", "raw_inventors.csv",
              "raw_companies.csv", "raw_relationships.csv"]:
    path = f"{DATA_DIR}/{fname}"
    if os.path.exists(path):
        kb = os.path.getsize(path) / 1024
        print(f"    {fname:35s} {kb:8.1f} KB")
print("\n  Next step: python scripts/02_clean_data.py")
print("=" * 60)