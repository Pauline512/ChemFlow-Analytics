"""
06_visualizations.py
====================
Generates 5 charts from the patents database.
Saves all charts to reports/charts/

Charts:
  1. Top 10 Inventors      — horizontal bar chart
  2. Top 10 Companies      — vertical bar chart
  3. Patents Per Year       — line chart with fill
  4. Country Distribution  — pie chart
  5. Inventors by Country  — bar chart with country labels
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use("Agg")   # non-interactive backend — safe for all environments
import os

# ── SETUP ─────────────────────────────────────────────────────────────────────
os.makedirs("reports/charts", exist_ok=True)

COLORS = {
    "green":      "#0F6E56",
    "blue":       "#1A6B9A",
    "teal":       "#2A9D8F",
    "orange":     "#F4A261",
    "red":        "#E76F51",
    "yellow":     "#E9C46A",
    "dark":       "#264653",
    "light_blue": "#A8DADC",
    "mid_blue":   "#457B9D",
    "navy":       "#1D3557",
}
PALETTE = list(COLORS.values())

def log(msg):
    print(f"[INFO] {msg}")

conn = sqlite3.connect("database/patents.db")
log("Connected to database/patents.db")


# ── CHART 1: Top 10 Inventors ─────────────────────────────────────────────────
log("Generating Chart 1: Top 10 Inventors...")

top_inventors = pd.read_sql_query("""
    SELECT
        i.name                          AS inventor_name,
        COUNT(DISTINCT r.patent_id)     AS patent_count
    FROM relationships r
    JOIN inventors i ON r.inventor_id = i.inventor_id
    GROUP BY i.inventor_id, i.name
    ORDER BY patent_count DESC
    LIMIT 10
""", conn)

fig, ax = plt.subplots(figsize=(12, 6))
ax.barh(top_inventors["inventor_name"], top_inventors["patent_count"],
        color=COLORS["green"], edgecolor="white")
ax.set_xlabel("Number of Patents", fontsize=12)
ax.set_title("Top 10 Inventors by Patent Count — Chemistry", fontsize=14, fontweight="bold")
ax.invert_yaxis()
for i, v in enumerate(top_inventors["patent_count"]):
    ax.text(v + 0.1, i, str(int(v)), va="center", fontsize=10)
plt.tight_layout()
plt.savefig("reports/charts/top_inventors.png", dpi=150)
plt.close()
log("  Saved: reports/charts/top_inventors.png")


# ── CHART 2: Top 10 Companies ─────────────────────────────────────────────────
log("Generating Chart 2: Top 10 Companies...")

top_companies = pd.read_sql_query("""
    SELECT
        c.name                          AS company_name,
        COUNT(DISTINCT r.patent_id)     AS patent_count
    FROM relationships r
    JOIN companies c ON r.company_id = c.company_id
    GROUP BY c.company_id, c.name
    ORDER BY patent_count DESC
    LIMIT 10
""", conn)

# Truncate long company names for display
top_companies["display_name"] = top_companies["company_name"].str[:30]

fig, ax = plt.subplots(figsize=(13, 6))
bars = ax.bar(top_companies["display_name"], top_companies["patent_count"],
              color=COLORS["blue"], edgecolor="white")
ax.set_xlabel("Company", fontsize=12)
ax.set_ylabel("Number of Patents", fontsize=12)
ax.set_title("Top 10 Companies by Patent Count — Chemistry", fontsize=14, fontweight="bold")
plt.xticks(rotation=40, ha="right", fontsize=9)
for bar in bars:
    ax.text(bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.1,
            str(int(bar.get_height())),
            ha="center", va="bottom", fontsize=9)
plt.tight_layout()
plt.savefig("reports/charts/top_companies.png", dpi=150)
plt.close()
log("  Saved: reports/charts/top_companies.png")


# ── CHART 3: Patents Per Year ─────────────────────────────────────────────────
log("Generating Chart 3: Patents Per Year...")

yearly = pd.read_sql_query("""
    SELECT year, COUNT(patent_id) AS patent_count
    FROM patents
    WHERE year IS NOT NULL
      AND year BETWEEN 1976 AND 2025
    GROUP BY year
    ORDER BY year ASC
""", conn)

fig, ax = plt.subplots(figsize=(14, 6))
ax.plot(yearly["year"], yearly["patent_count"],
        marker="o", color=COLORS["green"],
        linewidth=2, markersize=5, label="Patents")
ax.fill_between(yearly["year"], yearly["patent_count"],
                alpha=0.15, color=COLORS["green"])
ax.set_xlabel("Year", fontsize=12)
ax.set_ylabel("Number of Patents", fontsize=12)
ax.set_title("Chemistry Patent Trends Over Time", fontsize=14, fontweight="bold")
ax.grid(axis="y", linestyle="--", alpha=0.4)
plt.xticks(rotation=45, fontsize=8)
plt.tight_layout()
plt.savefig("reports/charts/yearly_trends.png", dpi=150)
plt.close()
log("  Saved: reports/charts/yearly_trends.png")


# ── CHART 4: Country Distribution ─────────────────────────────────────────────
log("Generating Chart 4: Country Distribution...")

top_countries = pd.read_sql_query("""
    SELECT
        i.country,
        COUNT(DISTINCT r.patent_id) AS patent_count
    FROM relationships r
    JOIN inventors i ON r.inventor_id = i.inventor_id
    WHERE i.country IS NOT NULL
      AND i.country NOT IN ('UNKNOWN', '')
    GROUP BY i.country
    ORDER BY patent_count DESC
    LIMIT 10
""", conn)

# Group small slices into "Other"
if len(top_countries) > 8:
    top8   = top_countries.head(8)
    other  = pd.DataFrame([{
        "country":      "Other",
        "patent_count": top_countries.iloc[8:]["patent_count"].sum()
    }])
    top_countries = pd.concat([top8, other], ignore_index=True)

fig, ax = plt.subplots(figsize=(10, 8))
wedges, texts, autotexts = ax.pie(
    top_countries["patent_count"],
    labels=top_countries["country"],
    autopct="%1.1f%%",
    colors=PALETTE[:len(top_countries)],
    startangle=140,
    pctdistance=0.82
)
for text in autotexts:
    text.set_fontsize(9)
ax.set_title("Chemistry Patent Distribution by Country", fontsize=14, fontweight="bold")
plt.tight_layout()
plt.savefig("reports/charts/country_distribution.png", dpi=150)
plt.close()
log("  Saved: reports/charts/country_distribution.png")


# ── CHART 5: Top Inventors with Country Labels ────────────────────────────────
log("Generating Chart 5: Top Inventors by Country...")

inventor_country = pd.read_sql_query("""
    SELECT
        i.name                          AS inventor_name,
        i.country,
        COUNT(DISTINCT r.patent_id)     AS patent_count
    FROM relationships r
    JOIN inventors i ON r.inventor_id = i.inventor_id
    GROUP BY i.inventor_id, i.name, i.country
    ORDER BY patent_count DESC
    LIMIT 10
""", conn)

inventor_country["display_name"] = inventor_country["inventor_name"].str[:20]

fig, ax = plt.subplots(figsize=(13, 6))
bars = ax.bar(inventor_country["display_name"], inventor_country["patent_count"],
              color=COLORS["teal"], edgecolor="white")
for bar, country in zip(bars, inventor_country["country"]):
    ax.text(bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.1,
            str(country),
            ha="center", va="bottom", fontsize=8, color="#333333")
ax.set_xlabel("Inventor", fontsize=12)
ax.set_ylabel("Number of Patents", fontsize=12)
ax.set_title("Top 10 Inventors with Country Labels — Chemistry", fontsize=14, fontweight="bold")
plt.xticks(rotation=40, ha="right", fontsize=9)
plt.tight_layout()
plt.savefig("reports/charts/inventors_by_country.png", dpi=150)
plt.close()
log("  Saved: reports/charts/inventors_by_country.png")


# ── DONE ──────────────────────────────────────────────────────────────────────
conn.close()

print("\n" + "=" * 55)
print("  ALL CHARTS SAVED TO reports/charts/")
print("=" * 55)
charts = [
    "top_inventors.png",
    "top_companies.png",
    "yearly_trends.png",
    "country_distribution.png",
    "inventors_by_country.png",
]
for chart in charts:
    path = f"reports/charts/{chart}"
    if os.path.exists(path):
        size = os.path.getsize(path) / 1024
        print(f"  {chart:<35} {size:6.1f} KB")
print("\n  Pipeline complete! All steps done.")
print("  Now push everything to GitHub.")
print("=" * 55)