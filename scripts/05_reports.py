"""
05_reports.py
=============
Generates all 3 required report types:

  A. Console Report  — formatted terminal output
  B. CSV Reports     — top_inventors.csv, top_companies.csv,
                       country_trends.csv, yearly_trends.csv
  C. JSON Report     — patent_report.json

Topic: Chemistry Patents (CPC Section C)
"""

import sqlite3
import pandas as pd
import json
import os

# ── SETUP ─────────────────────────────────────────────────────────────────────
os.makedirs("reports", exist_ok=True)

def log(msg):
    print(f"[INFO] {msg}")

conn = sqlite3.connect("database/patents.db")
log("Connected to database/patents.db")

# ── FETCH ALL DATA ─────────────────────────────────────────────────────────────
total_patents = pd.read_sql_query(
    "SELECT COUNT(*) AS n FROM patents", conn).iloc[0]["n"]

total_inventors = pd.read_sql_query(
    "SELECT COUNT(*) AS n FROM inventors", conn).iloc[0]["n"]

total_companies = pd.read_sql_query(
    "SELECT COUNT(*) AS n FROM companies", conn).iloc[0]["n"]

top_inventors = pd.read_sql_query("""
    SELECT
        i.name                          AS name,
        i.country,
        COUNT(DISTINCT r.patent_id)     AS patents
    FROM relationships r
    JOIN inventors i ON r.inventor_id = i.inventor_id
    GROUP BY i.inventor_id, i.name, i.country
    ORDER BY patents DESC
    LIMIT 10
""", conn)

top_companies = pd.read_sql_query("""
    SELECT
        c.name                          AS name,
        COUNT(DISTINCT r.patent_id)     AS patents
    FROM relationships r
    JOIN companies c ON r.company_id = c.company_id
    GROUP BY c.company_id, c.name
    ORDER BY patents DESC
    LIMIT 10
""", conn)

top_countries = pd.read_sql_query("""
    SELECT
        i.country,
        COUNT(DISTINCT r.patent_id)     AS patents,
        COUNT(DISTINCT i.inventor_id)   AS inventors
    FROM relationships r
    JOIN inventors i ON r.inventor_id = i.inventor_id
    WHERE i.country IS NOT NULL
      AND i.country NOT IN ('UNKNOWN', '')
    GROUP BY i.country
    ORDER BY patents DESC
    LIMIT 20
""", conn)

yearly_trends = pd.read_sql_query("""
    SELECT
        year,
        COUNT(patent_id)    AS patents
    FROM patents
    WHERE year IS NOT NULL
      AND year BETWEEN 1976 AND 2025
    GROUP BY year
    ORDER BY year ASC
""", conn)

conn.close()

# Compute country share (% of total patents)
total_country_patents = top_countries["patents"].sum()
top_countries["share"] = (
    top_countries["patents"] / total_country_patents
).round(4)


# ══════════════════════════════════════════════════════════════════════════════
# A. CONSOLE REPORT
# ══════════════════════════════════════════════════════════════════════════════
print()
print("=" * 52)
print("            CHEMISTRY PATENT REPORT")
print("         ChemFlow Analytics Pipeline")
print("         Topic: CPC Section C Patents")
print("=" * 52)

print(f"\nTotal Patents   : {int(total_patents):,}")
print(f"Total Inventors : {int(total_inventors):,}")
print(f"Total Companies : {int(total_companies):,}")

print("\nTop Inventors:")
for i, row in top_inventors.iterrows():
    print(f"  {i+1:>2}. {row['name']} ({row['country']}) — {int(row['patents'])} patents")

print("\nTop Companies:")
for i, row in top_companies.iterrows():
    print(f"  {i+1:>2}. {row['name']} — {int(row['patents'])} patents")

print("\nTop Countries:")
for i, row in top_countries.iterrows():
    pct = row["share"] * 100
    print(f"  {i+1:>2}. {row['country']:<12} {int(row['patents']):>6} patents  ({pct:.1f}%)")

print("\nPatents Per Year:")
max_p = yearly_trends["patents"].max() if len(yearly_trends) > 0 else 1
for _, row in yearly_trends.iterrows():
    bar = "█" * int((row["patents"] / max_p) * 25)
    print(f"  {int(row['year'])}: {bar:<25} {int(row['patents'])}")

print("=" * 52)


# ══════════════════════════════════════════════════════════════════════════════
# B. CSV REPORTS
# ══════════════════════════════════════════════════════════════════════════════
top_inventors.to_csv("reports/top_inventors.csv",  index=False)
top_companies.to_csv("reports/top_companies.csv",  index=False)
top_countries.to_csv("reports/country_trends.csv", index=False)
yearly_trends.to_csv("reports/yearly_trends.csv",  index=False)

print()
log("CSV reports saved:")
for f in ["top_inventors.csv", "top_companies.csv",
          "country_trends.csv", "yearly_trends.csv"]:
    path = f"reports/{f}"
    rows = pd.read_csv(path).shape[0]
    log(f"  {path}  ({rows} rows)")


# ══════════════════════════════════════════════════════════════════════════════
# C. JSON REPORT
# ══════════════════════════════════════════════════════════════════════════════
report = {
    "report_title":   "Chemistry Patent Intelligence Report",
    "topic":          "Chemistry (CPC Section C)",
    "total_patents":  int(total_patents),
    "total_inventors": int(total_inventors),
    "total_companies": int(total_companies),
    "top_inventors": [
        {
            "rank":    i + 1,
            "name":    row["name"],
            "country": row["country"],
            "patents": int(row["patents"])
        }
        for i, row in top_inventors.iterrows()
    ],
    "top_companies": [
        {
            "rank":    i + 1,
            "name":    row["name"],
            "patents": int(row["patents"])
        }
        for i, row in top_companies.iterrows()
    ],
    "top_countries": [
        {
            "country":   row["country"],
            "patents":   int(row["patents"]),
            "inventors": int(row["inventors"]),
            "share":     float(row["share"])
        }
        for _, row in top_countries.iterrows()
    ],
    "yearly_trends": [
        {
            "year":    int(row["year"]),
            "patents": int(row["patents"])
        }
        for _, row in yearly_trends.iterrows()
    ]
}

with open("reports/patent_report.json", "w", encoding="utf-8") as f:
    json.dump(report, f, indent=4)

log("JSON report saved: reports/patent_report.json")

# ── FINAL SUMMARY ──────────────────────────────────────────────────────────────
print()
print("=" * 52)
print("  ALL REPORTS GENERATED SUCCESSFULLY ✔")
print("  Output files:")
for f in ["reports/top_inventors.csv",
          "reports/top_companies.csv",
          "reports/country_trends.csv",
          "reports/yearly_trends.csv",
          "reports/patent_report.json"]:
    if os.path.exists(f):
        kb = os.path.getsize(f) / 1024
        print(f"    {f:<40} {kb:6.1f} KB")
print()
print("  Next step: python scripts/06_visualizations.py")
print("=" * 52)