# Global Patent Intelligence Data Pipeline

A data engineering project that collects, cleans, stores, and analyzes patent data.

## Project Structure

    Mini Project/
    ├── data/
    │   ├── raw_patents.csv
    │   ├── raw_inventors.csv
    │   ├── raw_companies.csv
    │   ├── raw_relationships.csv
    │   ├── clean_patents.csv
    │   ├── clean_inventors.csv
    │   ├── clean_companies.csv
    │   └── clean_relationships.csv
    ├── scripts/
    │   ├── 01_download_data.py
    │   ├── 02_clean_data.py
    │   ├── 03_load_database.py
    │   ├── 04_queries.py
    │   └── 05_reports.py
    ├── database/
    │   ├── schema.sql
    │   └── patents.db
    ├── reports/
    │   ├── top_inventors.csv
    │   ├── top_companies.csv
    │   ├── country_trends.csv
    │   ├── yearly_trends.csv
    │   └── patent_report.json
    └── README.md

## Pipeline Steps
1. `scripts/01_download_data.py` — Generate sample patent data
2. `scripts/02_clean_data.py` — Clean and validate data using pandas
3. `scripts/03_load_database.py` — Load data into SQLite database
4. `scripts/04_queries.py` — Run SQL queries for analysis
5. `scripts/05_reports.py` — Generate reports (console, CSV, JSON)

## How to Run
```bash
pip install pandas requests
python scripts/01_download_data.py
python scripts/02_clean_data.py
python scripts/03_load_database.py
python scripts/04_queries.py
python scripts/05_reports.py
```

## Technologies Used
- Python
- pandas
- SQLite
- GitHub