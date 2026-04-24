import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import os

os.makedirs("reports/charts", exist_ok=True)

conn = sqlite3.connect("database/patents.db")

# ── CHART 1: Top 10 Inventors ─────────────────────────────
top_inventors = pd.read_sql_query("""
    SELECT i.name, COUNT(r.patent_id) AS patents
    FROM relationships r
    JOIN inventors i ON r.inventor_id = i.inventor_id
    GROUP BY i.inventor_id
    ORDER BY patents DESC
    LIMIT 10
""", conn)

plt.figure(figsize=(12, 6))
plt.barh(top_inventors["name"], top_inventors["patents"], color="#0F6E56")
plt.xlabel("Number of Patents")
plt.title("Top 10 Inventors by Patent Count")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig("reports/charts/top_inventors.png")
plt.close()
print("Chart 1 saved: top_inventors.png")

# ── CHART 2: Top 10 Companies ─────────────────────────────
top_companies = pd.read_sql_query("""
    SELECT c.name, COUNT(r.patent_id) AS patents
    FROM relationships r
    JOIN companies c ON r.company_id = c.company_id
    GROUP BY c.company_id
    ORDER BY patents DESC
    LIMIT 10
""", conn)

plt.figure(figsize=(12, 6))
plt.bar(top_companies["name"], top_companies["patents"], color="#1A6B9A")
plt.xlabel("Company")
plt.ylabel("Number of Patents")
plt.title("Top 10 Companies by Patent Count")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig("reports/charts/top_companies.png")
plt.close()
print("Chart 2 saved: top_companies.png")

# ── CHART 3: Patents Per Year (Line Chart) ────────────────
yearly = pd.read_sql_query("""
    SELECT year, COUNT(patent_id) AS patents
    FROM patents
    GROUP BY year
    ORDER BY year ASC
""", conn)

plt.figure(figsize=(12, 6))
plt.plot(yearly["year"], yearly["patents"], marker="o", 
         color="#0F6E56", linewidth=2, markersize=6)
plt.fill_between(yearly["year"], yearly["patents"], alpha=0.2, color="#0F6E56")
plt.xlabel("Year")
plt.ylabel("Number of Patents")
plt.title("Patent Trends Over Time (2015-2024)")
plt.xticks(yearly["year"], rotation=45)
plt.grid(axis="y", linestyle="--", alpha=0.5)
plt.tight_layout()
plt.savefig("reports/charts/yearly_trends.png")
plt.close()
print("Chart 3 saved: yearly_trends.png")

# ── CHART 4: Top Countries (Pie Chart) ───────────────────
top_countries = pd.read_sql_query("""
    SELECT i.country, COUNT(r.patent_id) AS patents
    FROM relationships r
    JOIN inventors i ON r.inventor_id = i.inventor_id
    GROUP BY i.country
    ORDER BY patents DESC
""", conn)

colors = ["#0F6E56","#1A6B9A","#F4A261","#E76F51","#2A9D8F",
          "#E9C46A","#264653","#A8DADC","#457B9D","#1D3557"]

plt.figure(figsize=(10, 8))
plt.pie(
    top_countries["patents"],
    labels=top_countries["country"],
    autopct="%1.1f%%",
    colors=colors,
    startangle=140
)
plt.title("Patent Distribution by Country")
plt.tight_layout()
plt.savefig("reports/charts/country_distribution.png")
plt.close()
print("Chart 4 saved: country_distribution.png")

# ── CHART 5: Top 10 Inventors by Country (Stacked) ───────
inventor_country = pd.read_sql_query("""
    SELECT i.name, i.country, COUNT(r.patent_id) AS patents
    FROM relationships r
    JOIN inventors i ON r.inventor_id = i.inventor_id
    GROUP BY i.inventor_id
    ORDER BY patents DESC
    LIMIT 10
""", conn)

plt.figure(figsize=(12, 6))
bars = plt.bar(inventor_country["name"], inventor_country["patents"], color="#2A9D8F")
for bar, country in zip(bars, inventor_country["country"]):
    plt.text(
        bar.get_x() + bar.get_width() / 2,
        bar.get_height() + 0.1,
        country,
        ha="center", va="bottom", fontsize=9, color="#333333"
    )
plt.xlabel("Inventor")
plt.ylabel("Number of Patents")
plt.title("Top 10 Inventors with Country Labels")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()
plt.savefig("reports/charts/inventors_by_country.png")
plt.close()
print("Chart 5 saved: inventors_by_country.png")

conn.close()
print("\nAll charts saved to reports/charts/")