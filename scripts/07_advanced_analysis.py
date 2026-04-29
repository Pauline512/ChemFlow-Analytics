"""
07_advanced_analysis.py
=======================
Advanced analysis of chemistry patent categories.
Analyzes CPC subgroups, innovation trends, productivity,
and collaboration patterns.

Outputs saved to reports/:
  advanced_analysis.json
  category_analysis.csv
  inventor_productivity.csv
  innovation_trends.csv
"""

import sqlite3
import pandas as pd
import json
import os

# ── SETUP ─────────────────────────────────────────────────────────────────────
os.makedirs("reports", exist_ok=True)

def log(msg):
    print(f"[INFO] {msg}")

def section(title):
    print(f"\n{'=' * 58}")
    print(f"  {title}")
    print(f"{'=' * 58}")

conn = sqlite3.connect("database/patents.db")
log("Connected to database/patents.db")

# ══════════════════════════════════════════════════════════════════════════════
section("ANALYSIS 1 — Patent Output by Decade")
# ══════════════════════════════════════════════════════════════════════════════

decade_df = pd.read_sql_query("""
    SELECT
        (year / 10) * 10        AS decade,
        COUNT(patent_id)        AS patent_count,
        COUNT(DISTINCT year)    AS years_active
    FROM patents
    WHERE year IS NOT NULL
      AND year BETWEEN 1976 AND 2025
    GROUP BY decade
    ORDER BY decade ASC
""", conn)

decade_df["avg_per_year"] = (
    decade_df["patent_count"] / decade_df["years_active"]
).round(1)

print(decade_df.to_string(index=False))
decade_df.to_csv("reports/decade_analysis.csv", index=False)
log("Saved: reports/decade_analysis.csv")

# ══════════════════════════════════════════════════════════════════════════════
section("ANALYSIS 2 — Inventor Productivity")
# ══════════════════════════════════════════════════════════════════════════════
# Classifies inventors into tiers based on patent count

productivity_df = pd.read_sql_query("""
    SELECT
        i.inventor_id,
        i.name,
        i.country,
        COUNT(DISTINCT r.patent_id) AS patent_count
    FROM inventors i
    JOIN relationships r ON i.inventor_id = r.inventor_id
    GROUP BY i.inventor_id, i.name, i.country
    ORDER BY patent_count DESC
""", conn)

# Tier classification
def classify_tier(n):
    if n >= 10:  return "Elite (10+)"
    elif n >= 5: return "Prolific (5-9)"
    elif n >= 2: return "Active (2-4)"
    else:        return "Single (1)"

productivity_df["tier"] = productivity_df["patent_count"].apply(classify_tier)

tier_summary = productivity_df.groupby("tier").agg(
    inventor_count=("inventor_id", "count"),
    total_patents=("patent_count", "sum"),
    avg_patents=("patent_count", "mean")
).round(2).reset_index()

print("\nInventor Productivity Tiers:")
print(tier_summary.to_string(index=False))

productivity_df.to_csv("reports/inventor_productivity.csv", index=False)
log("Saved: reports/inventor_productivity.csv")

# ══════════════════════════════════════════════════════════════════════════════
section("ANALYSIS 3 — Country Innovation Trends Over Time")
# ══════════════════════════════════════════════════════════════════════════════

country_year_df = pd.read_sql_query("""
    SELECT
        p.year,
        i.country,
        COUNT(DISTINCT p.patent_id) AS patents
    FROM patents p
    JOIN relationships r ON p.patent_id  = r.patent_id
    JOIN inventors     i ON r.inventor_id = i.inventor_id
    WHERE p.year IS NOT NULL
      AND p.year BETWEEN 1990 AND 2025
      AND i.country NOT IN ('UNKNOWN', '')
      AND i.country IS NOT NULL
    GROUP BY p.year, i.country
    ORDER BY p.year ASC, patents DESC
""", conn)

# Top 5 countries overall
top5_countries = (
    country_year_df.groupby("country")["patents"]
    .sum().nlargest(5).index.tolist()
)

innovation_trends = country_year_df[
    country_year_df["country"].isin(top5_countries)
].copy()

print(f"\nTop 5 countries tracked: {top5_countries}")
print(innovation_trends.head(15).to_string(index=False))

innovation_trends.to_csv("reports/innovation_trends.csv", index=False)
log("Saved: reports/innovation_trends.csv")

# ══════════════════════════════════════════════════════════════════════════════
section("ANALYSIS 4 — Company vs Independent Inventors")
# ══════════════════════════════════════════════════════════════════════════════

# Patents with a company assigned vs without
assigned_df = pd.read_sql_query("""
    SELECT
        CASE
            WHEN r.company_id IS NOT NULL THEN 'Corporate'
            ELSE 'Independent'
        END AS inventor_type,
        COUNT(DISTINCT r.patent_id) AS patents
    FROM relationships r
    GROUP BY inventor_type
""", conn)

print("\nCorporate vs Independent Inventors:")
print(assigned_df.to_string(index=False))

# ══════════════════════════════════════════════════════════════════════════════
section("ANALYSIS 5 — Most Recent Chemistry Patents (Last 5 Years)")
# ══════════════════════════════════════════════════════════════════════════════

recent_df = pd.read_sql_query("""
    SELECT
        p.patent_id,
        p.title,
        p.year,
        i.name      AS inventor,
        i.country,
        c.name      AS company
    FROM patents p
    JOIN relationships r ON p.patent_id   = r.patent_id
    JOIN inventors     i ON r.inventor_id  = i.inventor_id
    LEFT JOIN companies c ON r.company_id  = c.company_id
    WHERE p.year >= 2020
    ORDER BY p.year DESC, p.patent_id
    LIMIT 20
""", conn)

print(f"\nMost Recent Patents (2020–2025): {len(recent_df)} found")
print(recent_df[["title", "year", "inventor", "country", "company"]]
      .head(10).to_string(index=False))

recent_df.to_csv("reports/recent_patents.csv", index=False)
log("Saved: reports/recent_patents.csv")

# ══════════════════════════════════════════════════════════════════════════════
# SAVE FULL ADVANCED ANALYSIS AS JSON
# ══════════════════════════════════════════════════════════════════════════════
analysis = {
    "topic": "Chemistry Patents — Advanced Analysis",
    "decade_analysis": decade_df.to_dict(orient="records"),
    "inventor_tiers":  tier_summary.to_dict(orient="records"),
    "top5_countries":  top5_countries,
    "corporate_vs_independent": assigned_df.to_dict(orient="records"),
    "recent_patents_count": len(recent_df),
}

with open("reports/advanced_analysis.json", "w", encoding="utf-8") as f:
    json.dump(analysis, f, indent=4)

log("Saved: reports/advanced_analysis.json")

conn.close()

print("\n" + "=" * 58)
print("  ADVANCED ANALYSIS COMPLETE ✔")
print("  Files saved to reports/:")
for f in ["decade_analysis.csv", "inventor_productivity.csv",
          "innovation_trends.csv", "recent_patents.csv",
          "advanced_analysis.json"]:
    path = f"reports/{f}"
    if os.path.exists(path):
        kb = os.path.getsize(path) / 1024
        print(f"    {f:<35} {kb:6.1f} KB")
print("\n  Next step: python scripts/dashboard.py")
print("=" * 58)