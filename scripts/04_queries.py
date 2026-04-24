import sqlite3
import pandas as pd

conn = sqlite3.connect("database/patents.db")

print("=" * 50)
print("Q1: TOP INVENTORS (most patents)")
print("=" * 50)
q1 = """
SELECT i.name, i.country, COUNT(r.patent_id) AS patent_count
FROM relationships r
JOIN inventors i ON r.inventor_id = i.inventor_id
GROUP BY i.inventor_id
ORDER BY patent_count DESC
LIMIT 10;
"""
df1 = pd.read_sql_query(q1, conn)
print(df1.to_string(index=False))

print("\n" + "=" * 50)
print("Q2: TOP COMPANIES (most patents)")
print("=" * 50)
q2 = """
SELECT c.name, COUNT(r.patent_id) AS patent_count
FROM relationships r
JOIN companies c ON r.company_id = c.company_id
GROUP BY c.company_id
ORDER BY patent_count DESC
LIMIT 10;
"""
df2 = pd.read_sql_query(q2, conn)
print(df2.to_string(index=False))

print("\n" + "=" * 50)
print("Q3: TOP COUNTRIES (most patents)")
print("=" * 50)
q3 = """
SELECT i.country, COUNT(r.patent_id) AS patent_count
FROM relationships r
JOIN inventors i ON r.inventor_id = i.inventor_id
GROUP BY i.country
ORDER BY patent_count DESC;
"""
df3 = pd.read_sql_query(q3, conn)
print(df3.to_string(index=False))

print("\n" + "=" * 50)
print("Q4: PATENTS PER YEAR (trends over time)")
print("=" * 50)
q4 = """
SELECT year, COUNT(patent_id) AS patent_count
FROM patents
GROUP BY year
ORDER BY year ASC;
"""
df4 = pd.read_sql_query(q4, conn)
print(df4.to_string(index=False))

print("\n" + "=" * 50)
print("Q5: JOIN QUERY (patents + inventors + companies)")
print("=" * 50)
q5 = """
SELECT p.patent_id, p.title, p.year, i.name AS inventor, 
       i.country, c.name AS company
FROM patents p
JOIN relationships r ON p.patent_id = r.patent_id
JOIN inventors i ON r.inventor_id = i.inventor_id
JOIN companies c ON r.company_id = c.company_id
LIMIT 10;
"""
df5 = pd.read_sql_query(q5, conn)
print(df5.to_string(index=False))

print("\n" + "=" * 50)
print("Q6: CTE QUERY (patents above average per company)")
print("=" * 50)
q6 = """
WITH company_counts AS (
    SELECT c.name AS company, COUNT(r.patent_id) AS patent_count
    FROM relationships r
    JOIN companies c ON r.company_id = c.company_id
    GROUP BY c.company_id
),
avg_count AS (
    SELECT AVG(patent_count) AS avg_patents
    FROM company_counts
)
SELECT company, patent_count, ROUND(avg_patents, 2) AS avg_patents
FROM company_counts, avg_count
WHERE patent_count > avg_patents
ORDER BY patent_count DESC;
"""
df6 = pd.read_sql_query(q6, conn)
print(df6.to_string(index=False))

print("\n" + "=" * 50)
print("Q7: RANKING QUERY (inventors ranked by patents)")
print("=" * 50)
q7 = """
SELECT 
    i.name,
    i.country,
    COUNT(r.patent_id) AS patent_count,
    RANK() OVER (ORDER BY COUNT(r.patent_id) DESC) AS rank
FROM relationships r
JOIN inventors i ON r.inventor_id = i.inventor_id
GROUP BY i.inventor_id
ORDER BY rank
LIMIT 10;
"""
df7 = pd.read_sql_query(q7, conn)
print(df7.to_string(index=False))

conn.close()
print("\nAll queries executed successfully!")