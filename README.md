# ⚗️ ChemFlow Analytics
### Global Patent Intelligence Data Pipeline

> A complete, reproducible data engineering pipeline that collects, cleans, stores, and analyses real-world **USPTO chemistry patent data** — built as part of a Cloud Computing & Big Data mini project.

---

## 📌 What This Project Does

This pipeline works like a real data engineering system. It pulls patent data from the **PatentsView** database (published by the US Patent and Trademark Office), filters it to the **Chemistry & Metallurgy** domain (CPC Section C), cleans it, loads it into a relational database, and generates reports, charts, and an interactive dashboard.

**In numbers:**
- 🗂️ **1,000** chemistry patents processed
- 👤 **288** inventors across 10+ countries
- 🏢 **15** major companies (assignees)
- 📊 **7** SQL queries (including CTEs and window functions)
- 📁 **8** Python scripts | **1,600+** lines of code

---

## 🏗️ Pipeline Architecture

```
USPTO / PatentsView (S3)
         │
         ▼
┌─────────────────────┐
│ 01_download_data.py │  — Streams TSV.zip files, filters by CPC section C
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│  02_clean_data.py   │  — Deduplicates, fills nulls, standardises formats
└────────┬────────────┘
         │
         ▼
┌──────────────────────┐
│ 03_load_database.py  │  — Inserts into SQLite with foreign key constraints
└────────┬─────────────┘
         │
         ▼
┌─────────────────────┐
│   04_queries.py     │  — Runs Q1–Q7 (JOINs, CTEs, window functions)
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│   05_reports.py     │  — Console report + CSV files + JSON report
└────────┬────────────┘
         │
         ▼
┌──────────────────────────┐
│  06_visualizations.py    │  — 5 matplotlib charts saved to reports/charts/
└────────┬─────────────────┘
         │
         ▼
┌─────────────────────────────┐
│  07_advanced_analysis.py    │  — Decade trends, productivity tiers, index
└────────┬────────────────────┘
         │
         ▼
┌─────────────────┐
│  dashboard.py   │  — Interactive Streamlit + Plotly dashboard
└─────────────────┘
```

---

## 📂 Project Structure

```
ChemFlow-Analytics/
│
├── scripts/
│   ├── 01_download_data.py        # Downloads & filters USPTO PatentsView data
│   ├── 02_clean_data.py           # Cleans raw CSVs with pandas
│   ├── 03_load_database.py        # Loads clean data into SQLite
│   ├── 04_queries.py              # Runs all 7 SQL queries (Q1–Q7)
│   ├── 05_reports.py              # Console + CSV + JSON reports
│   ├── 06_visualizations.py       # 5 matplotlib charts
│   ├── 07_advanced_analysis.py    # Advanced pattern analysis
│   └── read_data.py               # Utility: inspect raw data files
│
├── data/
│   ├── raw_patents.csv            # Downloaded from PatentsView
│   ├── raw_inventors.csv
│   ├── raw_companies.csv
│   ├── raw_relationships.csv
│   ├── clean_patents.csv          # After pandas cleaning
│   ├── clean_inventors.csv
│   ├── clean_companies.csv
│   └── clean_relationships.csv
│
├── database/
│   ├── schema.sql                 # Table definitions with foreign keys
│   └── patents.db                 # SQLite database (auto-generated)
│
├── reports/
│   ├── top_inventors.csv          # Q1 output
│   ├── top_companies.csv          # Q2 output
│   ├── country_trends.csv         # Q3 output
│   ├── yearly_trends.csv          # Q4 output
│   ├── patents_full_join.csv      # Q5 output
│   ├── top_inventor_per_country.csv  # Q6 CTE output
│   ├── inventor_rankings.csv      # Q7 window function output
│   ├── patent_report.json         # Full JSON summary report
│   ├── advanced_analysis.json
│   └── charts/
│       ├── top_inventors.png
│       ├── top_companies.png
│       ├── yearly_trends.png
│       ├── country_distribution.png
│       └── inventors_by_country.png
│
└── dashboard.py                   # Streamlit interactive dashboard
```

---

## 🗄️ Database Schema

```sql
-- Core tables
CREATE TABLE patents (
    patent_id   TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    abstract    TEXT,
    filing_date TEXT,
    year        INTEGER
);

CREATE TABLE inventors (
    inventor_id TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    country     TEXT
);

CREATE TABLE companies (
    company_id  TEXT PRIMARY KEY,
    name        TEXT NOT NULL
);

-- Junction table linking all three
CREATE TABLE relationships (
    patent_id   TEXT REFERENCES patents(patent_id),
    inventor_id TEXT REFERENCES inventors(inventor_id),
    company_id  TEXT REFERENCES companies(company_id)
);
```

---

## 🔍 SQL Queries (Q1–Q7)

| # | Query | SQL Feature Used |
|---|-------|-----------------|
| Q1 | Top inventors by patent count | `GROUP BY` + `COUNT DISTINCT` |
| Q2 | Top companies by patent count | `JOIN` + `GROUP BY` |
| Q3 | Countries producing the most patents | Multi-table `JOIN` + `WHERE` |
| Q4 | Patents filed per year (trend) | `GROUP BY year ORDER BY year` |
| Q5 | Full patent details (patents + inventors + companies) | 3-table `JOIN` |
| Q6 | Top inventor per country | `WITH` (CTE) + `RANK() OVER PARTITION BY` |
| Q7 | Global inventor ranking with share % | `RANK`, `DENSE_RANK`, `NTILE`, `SUM OVER` |

---

## 📋 Reports Generated

### A. Console Report (Terminal)
Printed directly to the terminal with a formatted layout and ASCII bar charts showing patents per year.

### B. CSV Reports
| File | Contents |
|------|----------|
| `top_inventors.csv` | Name, country, patent count |
| `top_companies.csv` | Company name, patent count |
| `country_trends.csv` | Country, patents, inventors, share % |
| `yearly_trends.csv` | Year, patent count (2015–2024) |

### C. JSON Report (`patent_report.json`)
```json
{
  "total_patents": 1000,
  "top_inventors": [{"rank": 1, "name": "John Dupont", "patents": 8}],
  "top_companies": [{"rank": 1, "name": "Samsung", "patents": 74}],
  "top_countries": [{"country": "DE", "patents": 124, "share": 0.124}]
}
```

### D. Charts (5 PNG files)
- Top 10 Inventors — horizontal bar chart
- Top 10 Companies — vertical bar chart
- Patents Per Year — line chart with fill
- Country Distribution — pie chart
- Inventors by Country — grouped bar chart

### E. Advanced Analysis
- Patent output by decade with averages
- Inventor productivity tiers (Elite / Prolific / Active / Single)
- Innovation trend index (year-over-year growth)

---

## 🚀 How to Run

### 1. Install requirements
```bash
pip install pandas matplotlib plotly streamlit
```

### 2. Run the full pipeline (steps in order)
```bash
python scripts/01_download_data.py      # ~5–10 min (downloads from internet)
python scripts/02_clean_data.py
python scripts/03_load_database.py
python scripts/04_queries.py
python scripts/05_reports.py
python scripts/06_visualizations.py
python scripts/07_advanced_analysis.py
```

### 3. Launch the dashboard
```bash
streamlit run dashboard.py
```

> **Shortcut:** Steps 2–7 can be run immediately using the pre-cleaned data already committed to `data/`. Only Step 1 requires a live internet connection.

---

## 🌐 Data Source

| | |
|-|-|
| **Source** | PatentsView — USPTO Granted Patent Disambiguated Data |
| **URL** | https://data.patentsview.org/ |
| **Format** | TSV.zip files (streamed and decompressed in memory) |
| **Filter** | CPC Section C — Chemistry & Metallurgy (C01–C40) |
| **Cap** | 5,000 patent IDs (configurable via `MAX_PATENTS`) |

---

## 🛠️ Tools & Technologies

| Tool | Role |
|------|------|
| **Python 3** | Pipeline scripting |
| **pandas** | Data cleaning & transformation |
| **SQLite** | Relational database |
| **SQL** | Analytical queries (CTEs, window functions, JOINs) |
| **matplotlib** | Static data visualisations |
| **plotly** | Interactive charts in dashboard |
| **Streamlit** | Web-based interactive dashboard |
| **GitHub** | Version control & submission |

---

## 👩‍💻 Author

**Pauline Mbasani**  
Cloud Computing & Big Data — Mini Project  
*ChemFlow Analytics Pipeline*