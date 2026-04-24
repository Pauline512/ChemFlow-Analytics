import sqlite3
import pandas as pd
import json
import os

os.makedirs("reports", exist_ok=True)

conn = sqlite3.connect("database/patents.db")

# ── FETCH DATA ─────────────────────────────────────────────
total_patents = pd.read_sql_query("SELECT COUNT(*) AS total FROM patents", conn).iloc[0]["total"]

top_inventors = pd.read_sql_query("""
    SELECT i.name, i.country, COUNT(r.patent_id) AS patents
    FROM relationships r
    JOIN inventors i ON r.inventor_id = i.inventor_id
    GROUP BY i.inventor_id
    ORDER BY patents DESC
    LIMIT 10
""", conn)

top_companies = pd.read_sql_query("""
    SELECT c.name, COUNT(r.patent_id) AS patents
    FROM relationships r
    JOIN companies c ON r.company_id = c.company_id
    GROUP BY c.company_id
    ORDER BY patents DESC
    LIMIT 10
""", conn)

top_countries = pd.read_sql_query("""
    SELECT i.country, COUNT(r.patent_id) AS patents
    FROM relationships r
    JOIN inventors i ON r.inventor_id = i.inventor_id
    GROUP BY i.country
    ORDER BY patents DESC
""", conn)

yearly_trends = pd.read_sql_query("""
    SELECT year, COUNT(patent_id) AS patents
    FROM patents
    GROUP BY year
    ORDER BY year ASC
""", conn)

conn.close()

# ── CONSOLE REPORT ─────────────────────────────────────────
print("=" * 52)
print("          GLOBAL PATENT INTELLIGENCE REPORT")
print("=" * 52)

print(f"\nTotal Patents: {int(total_patents):,}")

print("\nTop Inventors:")
for i, row in top_inventors.iterrows():
    print(f"  {i+1}. {row['name']} ({row['country']}) — {int(row['patents'])} patents")

print("\nTop Companies:")
for i, row in top_companies.iterrows():
    print(f"  {i+1}. {row['name']} — {int(row['patents'])} patents")

print("\nTop Countries:")
for i, row in top_countries.iterrows():
    print(f"  {i+1}. {row['country']} — {int(row['patents'])} patents")

print("\nPatents Per Year:")
for _, row in yearly_trends.iterrows():
    bar = "█" * (int(row["patents"]) // 10)
    print(f"  {int(row['year'])}: {bar} {int(row['patents'])}")

print("=" * 52)

# ── CSV REPORTS ────────────────────────────────────────────
top_inventors.to_csv("reports/top_inventors.csv", index=False)
top_companies.to_csv("reports/top_companies.csv", index=False)
top_countries.to_csv("reports/country_trends.csv", index=False)
yearly_trends.to_csv("reports/yearly_trends.csv", index=False)

print("\nCSV reports saved:")
print("  reports/top_inventors.csv")
print("  reports/top_companies.csv")
print("  reports/country_trends.csv")
print("  reports/yearly_trends.csv")

# ── JSON REPORT ────────────────────────────────────────────
report = {
    "total_patents": int(total_patents),
    "top_inventors": [
        {"rank": i+1, "name": row["name"], "country": row["country"], "patents": int(row["patents"])}
        for i, row in top_inventors.iterrows()
    ],
    "top_companies": [
        {"rank": i+1, "name": row["name"], "patents": int(row["patents"])}
        for i, row in top_companies.iterrows()
    ],
    "top_countries": [
        {"country": row["country"], "patents": int(row["patents"])}
        for _, row in top_countries.iterrows()
    ],
    "yearly_trends": [
        {"year": int(row["year"]), "patents": int(row["patents"])}
        for _, row in yearly_trends.iterrows()
    ]
}

with open("reports/patent_report.json", "w") as f:
    json.dump(report, f, indent=4)

print("  reports/patent_report.json")
print("\nAll reports generated successfully!")