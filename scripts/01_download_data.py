import pandas as pd
import os
import random
from datetime import datetime, timedelta

os.makedirs("data", exist_ok=True)

random.seed(42)

# --- Sample data pools ---
companies = [
    "IBM", "Samsung", "Google LLC", "Microsoft", "Huawei",
    "Apple Inc", "Canon", "Sony", "Intel", "Qualcomm",
    "LG Electronics", "Bosch", "Siemens", "Toyota", "BASF"
]

countries = [
    "US", "CN", "JP", "KR", "DE",
    "FR", "GB", "TW", "CA", "IN"
]

first_names = [
    "John", "Wei", "Yuki", "Min", "Hans",
    "Alice", "James", "Li", "Kenji", "Park",
    "Emma", "Carlos", "Priya", "Omar", "Sofia"
]

last_names = [
    "Smith", "Zhang", "Tanaka", "Kim", "Mueller",
    "Dupont", "Johnson", "Wang", "Sato", "Lee",
    "Brown", "Garcia", "Patel", "Hassan", "Rossi"
]

tech_words = [
    "Wireless", "Optical", "Neural", "Quantum", "Semiconductor",
    "Biomedical", "Autonomous", "Encrypted", "Modular", "Photonic"
]

tech_topics = [
    "Communication System", "Data Processing Method", "Energy Storage Device",
    "Image Recognition Algorithm", "Drug Delivery Mechanism",
    "Sensor Array", "Battery Technology", "Display Panel",
    "Network Protocol", "Machine Learning Model"
]

def random_date(start_year=2015, end_year=2024):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    return (start + timedelta(days=random.randint(0, delta.days))).strftime("%Y-%m-%d")

def generate_abstract():
    return (
        f"A system and method for {random.choice(tech_topics).lower()} "
        f"comprising {random.choice(tech_words).lower()} components "
        f"configured to improve efficiency and performance in "
        f"{random.choice(tech_topics).lower()} applications."
    )

# --- Generate patents ---
print("Generating patents...")
num_patents = 1000
patents = []
for i in range(1, num_patents + 1):
    date = random_date()
    patents.append({
        "patent_id": f"US{9000000 + i}",
        "title": f"{random.choice(tech_words)} {random.choice(tech_topics)}",
        "abstract": generate_abstract(),
        "filing_date": date,
        "year": int(date[:4])
    })
patents_df = pd.DataFrame(patents)
patents_df.to_csv("data/raw_patents.csv", index=False)
print(f"  {len(patents_df)} patents saved to data/raw_patents.csv")

# --- Generate inventors ---
print("Generating inventors...")
num_inventors = 300
inventors = []
for i in range(1, num_inventors + 1):
    inventors.append({
        "inventor_id": f"INV{i:04d}",
        "name": f"{random.choice(first_names)} {random.choice(last_names)}",
        "country": random.choice(countries)
    })
inventors_df = pd.DataFrame(inventors)
inventors_df.to_csv("data/raw_inventors.csv", index=False)
print(f"  {len(inventors_df)} inventors saved to data/raw_inventors.csv")

# --- Generate companies ---
print("Generating companies...")
companies_data = []
for i, name in enumerate(companies, 1):
    companies_data.append({
        "company_id": f"COMP{i:03d}",
        "name": name
    })
companies_df = pd.DataFrame(companies_data)
companies_df.to_csv("data/raw_companies.csv", index=False)
print(f"  {len(companies_df)} companies saved to data/raw_companies.csv")

# --- Generate relationships ---
print("Generating relationships...")
relationships = []
for patent in patents:
    inventor = random.choice(inventors)
    company = random.choice(companies_data)
    relationships.append({
        "patent_id": patent["patent_id"],
        "inventor_id": inventor["inventor_id"],
        "company_id": company["company_id"]
    })
relationships_df = pd.DataFrame(relationships)
relationships_df.to_csv("data/raw_relationships.csv", index=False)
print(f"  {len(relationships_df)} relationships saved to data/raw_relationships.csv")

print("\nAll raw data files created successfully in data/")