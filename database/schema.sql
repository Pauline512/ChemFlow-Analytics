-- ============================================================
-- Global Patent Intelligence Database Schema
-- ChemFlow Analytics — Cloud Computing & Big Data Mini Project
-- ============================================================

-- Drop tables in reverse dependency order (relationships first)
DROP TABLE IF EXISTS relationships;
DROP TABLE IF EXISTS patents;
DROP TABLE IF EXISTS inventors;
DROP TABLE IF EXISTS companies;

-- Patents table
CREATE TABLE IF NOT EXISTS patents (
    patent_id   TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    abstract    TEXT,
    filing_date TEXT,
    year        INTEGER
);

-- Inventors table
CREATE TABLE IF NOT EXISTS inventors (
    inventor_id TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    country     TEXT
);

-- Companies (Assignees) table
CREATE TABLE IF NOT EXISTS companies (
    company_id  TEXT PRIMARY KEY,
    name        TEXT NOT NULL
);

-- Relationships table (links patents to inventors and companies)
CREATE TABLE IF NOT EXISTS relationships (
    patent_id   TEXT,
    inventor_id TEXT,
    company_id  TEXT,
    FOREIGN KEY (patent_id)   REFERENCES patents(patent_id),
    FOREIGN KEY (inventor_id) REFERENCES inventors(inventor_id),
    FOREIGN KEY (company_id)  REFERENCES companies(company_id)
);

-- CPC Classifications table (additional — chemistry topic context)
CREATE TABLE IF NOT EXISTS cpc_classifications (
    patent_id   TEXT,
    cpc_section TEXT,
    FOREIGN KEY (patent_id) REFERENCES patents(patent_id)
);