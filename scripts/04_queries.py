"""
04_queries.py
=============
Runs all 7 SQL queries against the patents SQLite database.
Prints results to console and saves each result as a CSV.

Q1 — Top Inventors       : Who has the most patents?
Q2 — Top Companies       : Which companies own the most patents?
Q3 — Countries           : Which countries produce the most patents?
Q4 — Trends Over Time    : How many patents per year?
Q5 — JOIN Query          : Patents with inventor and company names
Q6 — CTE Query           : Top inventors per country (WITH statement)
Q7 — Ranking Query       : Rank inventors using window functions
"""

import sqlite3
import pandas as pd
import os

# ── SETUP ─────────────────────────────────────────────────────────────────────
DB_PATH    = "database/patents.db"
OUTPUT_DIR = "reports"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def log(msg):
    print(f"\n{'=' * 60}")
    print(f"  {msg}")
    print(f"{'=' * 60}")

def run_query(conn, label, sql, output_file=None, top_n=10):
    """Run a SQL query, print results, optionally save to CSV."""
    print(f"\n--- {label} ---")
    df = pd.read_sql_query(sql, conn)
    if df.empty:
        print("  (no results returned)")
    else:
        print(df.head(top_n).to_string(index=False))
        print(f"\n  → {len(df):,} total rows returned")
    if output_file:
        path = f"{OUTPUT_DIR}/{output_file}"
        df.to_csv(path, index=False)
        print(f"  → Saved to {path}")
    return df

# ── CONNECT ───────────────────────────────────────────────────────────────────
print("Connecting to database...")
conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA foreign_keys = ON")
print(f"Connected → {DB_PATH}")

# Quick table check
for table in ["patents", "inventors", "companies", "relationships"]:
    count = pd.read_sql_query(f"SELECT COUNT(*) AS n FROM {table}", conn)["n"][0]
    print(f"  {table:20s}: {count:,} rows")


# ══════════════════════════════════════════════════════════════════════════════
log("Q1 — Top Inventors: Who has the most patents?")
# ══════════════════════════════════════════════════════════════════════════════

q1 = """
SELECT
    i.name                          AS inventor_name,
    i.country,
    COUNT(DISTINCT r.patent_id)     AS patent_count
FROM inventors i
JOIN relationships r ON i.inventor_id = r.inventor_id
GROUP BY i.inventor_id, i.name, i.country
ORDER BY patent_count DESC
LIMIT 20
"""
run_query(conn, "Q1 — Top Inventors", q1, "top_inventors.csv")


# ══════════════════════════════════════════════════════════════════════════════
log("Q2 — Top Companies: Which companies own the most patents?")
# ══════════════════════════════════════════════════════════════════════════════

q2 = """
SELECT
    c.name                          AS company_name,
    COUNT(DISTINCT r.patent_id)     AS patent_count
FROM companies c
JOIN relationships r ON c.company_id = r.company_id
GROUP BY c.company_id, c.name
ORDER BY patent_count DESC
LIMIT 20
"""
run_query(conn, "Q2 — Top Companies", q2, "top_companies.csv")


# ══════════════════════════════════════════════════════════════════════════════
log("Q3 — Countries: Which countries produce the most patents?")
# ══════════════════════════════════════════════════════════════════════════════

q3 = """
SELECT
    i.country,
    COUNT(DISTINCT r.patent_id)     AS patent_count,
    COUNT(DISTINCT i.inventor_id)   AS inventor_count
FROM inventors i
JOIN relationships r ON i.inventor_id = r.inventor_id
WHERE i.country IS NOT NULL
  AND i.country != 'UNKNOWN'
  AND i.country != ''
GROUP BY i.country
ORDER BY patent_count DESC
LIMIT 20
"""
run_query(conn, "Q3 — Countries", q3, "country_trends.csv")


# ══════════════════════════════════════════════════════════════════════════════
log("Q4 — Trends Over Time: How many patents per year?")
# ══════════════════════════════════════════════════════════════════════════════

q4 = """
SELECT
    year,
    COUNT(patent_id)    AS patent_count
FROM patents
WHERE year IS NOT NULL
  AND year BETWEEN 1976 AND 2025
GROUP BY year
ORDER BY year ASC
"""
run_query(conn, "Q4 — Yearly Trends", q4, "yearly_trends.csv", top_n=50)


# ══════════════════════════════════════════════════════════════════════════════
log("Q5 — JOIN Query: Patents with inventor and company names")
# ══════════════════════════════════════════════════════════════════════════════

q5 = """
SELECT
    p.patent_id,
    p.title,
    p.year,
    i.name          AS inventor_name,
    i.country       AS inventor_country,
    c.name          AS company_name
FROM patents p
JOIN relationships r ON p.patent_id  = r.patent_id
JOIN inventors     i ON r.inventor_id = i.inventor_id
JOIN companies     c ON r.company_id  = c.company_id
ORDER BY p.year DESC, p.patent_id
LIMIT 100
"""
run_query(conn, "Q5 — Full JOIN", q5, "patents_full_join.csv")


# ══════════════════════════════════════════════════════════════════════════════
log("Q6 — CTE Query: Top inventor per country (WITH statement)")
# ══════════════════════════════════════════════════════════════════════════════

q6 = """
WITH inventor_counts AS (
    -- Step 1: count patents per inventor
    SELECT
        i.inventor_id,
        i.name      AS inventor_name,
        i.country,
        COUNT(DISTINCT r.patent_id) AS patent_count
    FROM inventors i
    JOIN relationships r ON i.inventor_id = r.inventor_id
    WHERE i.country IS NOT NULL
      AND i.country != 'UNKNOWN'
      AND i.country != ''
    GROUP BY i.inventor_id, i.name, i.country
),
ranked AS (
    -- Step 2: rank inventors within each country
    SELECT
        inventor_name,
        country,
        patent_count,
        RANK() OVER (PARTITION BY country ORDER BY patent_count DESC) AS country_rank
    FROM inventor_counts
)
-- Step 3: keep only the top inventor per country
SELECT
    country,
    inventor_name,
    patent_count,
    country_rank
FROM ranked
WHERE country_rank = 1
ORDER BY patent_count DESC
LIMIT 30
"""
run_query(conn, "Q6 — CTE: Top Inventor Per Country", q6, "top_inventor_per_country.csv")


# ══════════════════════════════════════════════════════════════════════════════
log("Q7 — Ranking Query: Rank inventors using window functions")
# ══════════════════════════════════════════════════════════════════════════════

q7 = """
WITH inventor_stats AS (
    SELECT
        i.inventor_id,
        i.name          AS inventor_name,
        i.country,
        COUNT(DISTINCT r.patent_id) AS patent_count
    FROM inventors i
    JOIN relationships r ON i.inventor_id = r.inventor_id
    GROUP BY i.inventor_id, i.name, i.country
)
SELECT
    inventor_name,
    country,
    patent_count,
    RANK()        OVER (ORDER BY patent_count DESC) AS overall_rank,
    DENSE_RANK()  OVER (ORDER BY patent_count DESC) AS dense_rank,
    NTILE(4)      OVER (ORDER BY patent_count DESC) AS quartile,
    ROUND(
        100.0 * patent_count /
        SUM(patent_count) OVER (), 2
    )                                                AS pct_of_total
FROM inventor_stats
ORDER BY overall_rank
LIMIT 30
"""
run_query(conn, "Q7 — Inventor Rankings (Window Functions)", q7, "inventor_rankings.csv")


# ── DONE ──────────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("  ALL QUERIES COMPLETE")
print("  Reports saved to reports/:")
for f in ["top_inventors.csv", "top_companies.csv", "country_trends.csv",
          "yearly_trends.csv", "patents_full_join.csv",
          "top_inventor_per_country.csv", "inventor_rankings.csv"]:
    path = f"reports/{f}"
    if os.path.exists(path):
        rows = pd.read_csv(path).shape[0]
        print(f"    {f:40s} {rows:6,} rows")
print("\n  Next step: python scripts/05_reports.py")
print("=" * 60)

conn.close()