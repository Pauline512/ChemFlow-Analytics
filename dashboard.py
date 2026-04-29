"""
dashboard.py
============
Streamlit dashboard for ChemFlow Analytics.
Displays interactive charts and tables from the patents database.

Run with:
    streamlit run dashboard.py
"""

import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import os

# ── PAGE CONFIG ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="ChemFlow Analytics",
    page_icon="⚗️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── LOAD DATA ─────────────────────────────────────────────────────────────────
DB_PATH = "database/patents.db"

@st.cache_data
def load_data():
    if not os.path.exists(DB_PATH):
        return None
    conn = sqlite3.connect(DB_PATH)

    total_patents   = pd.read_sql_query("SELECT COUNT(*) AS n FROM patents",   conn).iloc[0]["n"]
    total_inventors = pd.read_sql_query("SELECT COUNT(*) AS n FROM inventors", conn).iloc[0]["n"]
    total_companies = pd.read_sql_query("SELECT COUNT(*) AS n FROM companies", conn).iloc[0]["n"]

    top_inventors = pd.read_sql_query("""
        SELECT i.name AS inventor, i.country,
               COUNT(DISTINCT r.patent_id) AS patents
        FROM relationships r
        JOIN inventors i ON r.inventor_id = i.inventor_id
        GROUP BY i.inventor_id, i.name, i.country
        ORDER BY patents DESC LIMIT 20
    """, conn)

    top_companies = pd.read_sql_query("""
        SELECT c.name AS company,
               COUNT(DISTINCT r.patent_id) AS patents
        FROM relationships r
        JOIN companies c ON r.company_id = c.company_id
        GROUP BY c.company_id, c.name
        ORDER BY patents DESC LIMIT 20
    """, conn)

    top_countries = pd.read_sql_query("""
        SELECT i.country,
               COUNT(DISTINCT r.patent_id)   AS patents,
               COUNT(DISTINCT i.inventor_id) AS inventors
        FROM relationships r
        JOIN inventors i ON r.inventor_id = i.inventor_id
        WHERE i.country NOT IN ('UNKNOWN', '') AND i.country IS NOT NULL
        GROUP BY i.country
        ORDER BY patents DESC LIMIT 20
    """, conn)

    yearly_trends = pd.read_sql_query("""
        SELECT year, COUNT(patent_id) AS patents
        FROM patents
        WHERE year BETWEEN 1976 AND 2025
        GROUP BY year ORDER BY year ASC
    """, conn)

    recent_patents = pd.read_sql_query("""
        SELECT p.patent_id, p.title, p.year,
               i.name AS inventor, i.country,
               c.name AS company
        FROM patents p
        JOIN relationships r ON p.patent_id   = r.patent_id
        JOIN inventors     i ON r.inventor_id  = i.inventor_id
        LEFT JOIN companies c ON r.company_id  = c.company_id
        WHERE p.year >= 2015
        ORDER BY p.year DESC LIMIT 100
    """, conn)

    decade_df = pd.read_sql_query("""
        SELECT (year / 10) * 10 AS decade,
               COUNT(patent_id) AS patents
        FROM patents
        WHERE year BETWEEN 1976 AND 2025
        GROUP BY decade ORDER BY decade
    """, conn)

    conn.close()
    return {
        "total_patents":   total_patents,
        "total_inventors": total_inventors,
        "total_companies": total_companies,
        "top_inventors":   top_inventors,
        "top_companies":   top_companies,
        "top_countries":   top_countries,
        "yearly_trends":   yearly_trends,
        "recent_patents":  recent_patents,
        "decade_df":       decade_df,
    }

data = load_data()

# ── SIDEBAR ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/emoji/96/alembic-emoji.png", width=60)
    st.title("ChemFlow Analytics")
    st.caption("Chemistry Patent Intelligence Pipeline")
    st.divider()

    st.markdown("**Topic:** CPC Section C")
    st.markdown("**Source:** USPTO PatentsView")
    st.divider()

    page = st.radio("Navigate", [
        "📊 Overview",
        "🏆 Top Inventors",
        "🏢 Top Companies",
        "🌍 Countries",
        "📈 Trends Over Time",
        "📄 Recent Patents",
    ])

    st.divider()
    if data:
        st.success("Database connected ✔")
    else:
        st.error("Database not found. Run the pipeline first.")

# ── GUARD ─────────────────────────────────────────────────────────────────────
if not data:
    st.error("⚠️ Database not found at `database/patents.db`")
    st.info("Run the full pipeline first:\n```\npython scripts/01_download_data.py\npython scripts/02_clean_data.py\npython scripts/03_load_database.py\n```")
    st.stop()

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
if page == "📊 Overview":
    st.title("⚗️ ChemFlow Analytics Dashboard")
    st.caption("Global Chemistry Patent Intelligence — CPC Section C")
    st.divider()

    # KPI cards
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Patents",   f"{int(data['total_patents']):,}")
    col2.metric("Total Inventors", f"{int(data['total_inventors']):,}")
    col3.metric("Total Companies", f"{int(data['total_companies']):,}")

    st.divider()
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Patents Per Year")
        fig = px.area(
            data["yearly_trends"], x="year", y="patents",
            color_discrete_sequence=["#0F6E56"],
            labels={"year": "Year", "patents": "Patents"}
        )
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Top 10 Countries")
        top10 = data["top_countries"].head(10)
        fig = px.bar(
            top10, x="patents", y="country",
            orientation="h",
            color="patents",
            color_continuous_scale="Teal",
            labels={"patents": "Patents", "country": "Country"}
        )
        fig.update_layout(margin=dict(t=20, b=20), coloraxis_showscale=False)
        fig.update_yaxes(autorange="reversed")
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Patents by Decade")
        fig = px.bar(
            data["decade_df"], x="decade", y="patents",
            color_discrete_sequence=["#1A6B9A"],
            labels={"decade": "Decade", "patents": "Patents"}
        )
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.subheader("Country Share (Top 8)")
        top8 = data["top_countries"].head(8)
        fig = px.pie(
            top8, values="patents", names="country",
            color_discrete_sequence=px.colors.qualitative.Set2,
            hole=0.4
        )
        fig.update_layout(margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: TOP INVENTORS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🏆 Top Inventors":
    st.title("🏆 Top Inventors")
    st.caption("Ranked by number of chemistry patents filed")
    st.divider()

    top_n = st.slider("Show top N inventors", 5, 20, 10)
    df = data["top_inventors"].head(top_n)

    fig = px.bar(
        df, x="patents", y="inventor",
        orientation="h",
        color="country",
        labels={"patents": "Patent Count", "inventor": "Inventor"},
        title=f"Top {top_n} Inventors by Patent Count"
    )
    fig.update_yaxes(autorange="reversed")
    fig.update_layout(legend_title="Country", height=500)
    st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("Inventor Data Table")
    st.dataframe(df.reset_index(drop=True), use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: TOP COMPANIES
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🏢 Top Companies":
    st.title("🏢 Top Companies")
    st.caption("Companies with the most chemistry patents")
    st.divider()

    top_n = st.slider("Show top N companies", 5, 20, 10)
    df = data["top_companies"].head(top_n)
    df["short_name"] = df["company"].str[:35]

    fig = px.bar(
        df, x="short_name", y="patents",
        color="patents",
        color_continuous_scale="Blues",
        labels={"short_name": "Company", "patents": "Patent Count"},
        title=f"Top {top_n} Companies by Patent Count"
    )
    fig.update_layout(coloraxis_showscale=False, xaxis_tickangle=-35, height=500)
    st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("Company Data Table")
    st.dataframe(df[["company", "patents"]].reset_index(drop=True),
                 use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: COUNTRIES
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🌍 Countries":
    st.title("🌍 Countries")
    st.caption("Which countries produce the most chemistry patents?")
    st.divider()

    df = data["top_countries"].copy()
    total = df["patents"].sum()
    df["share (%)"] = (df["patents"] / total * 100).round(2)

    col1, col2 = st.columns(2)

    with col1:
        fig = px.bar(
            df.head(15), x="country", y="patents",
            color="patents", color_continuous_scale="Teal",
            title="Top 15 Countries by Patent Count",
            labels={"country": "Country", "patents": "Patents"}
        )
        fig.update_layout(coloraxis_showscale=False, xaxis_tickangle=-35)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.scatter(
            df.head(15),
            x="inventors", y="patents",
            size="patents", color="country",
            title="Inventors vs Patents per Country",
            labels={"inventors": "No. of Inventors", "patents": "No. of Patents"},
            hover_name="country"
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("Country Summary Table")
    st.dataframe(df.reset_index(drop=True), use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: TRENDS OVER TIME
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📈 Trends Over Time":
    st.title("📈 Patent Trends Over Time")
    st.caption("How chemistry patent filing has changed year by year")
    st.divider()

    df = data["yearly_trends"].copy()

    # Year range filter
    min_y, max_y = int(df["year"].min()), int(df["year"].max())
    year_range = st.slider("Select year range", min_y, max_y, (min_y, max_y))
    df = df[(df["year"] >= year_range[0]) & (df["year"] <= year_range[1])]

    fig = px.area(
        df, x="year", y="patents",
        color_discrete_sequence=["#0F6E56"],
        labels={"year": "Year", "patents": "Patents Filed"},
        title="Chemistry Patents Per Year"
    )
    fig.update_traces(line_width=2)
    fig.update_layout(height=450)
    st.plotly_chart(fig, use_container_width=True)

    # Summary stats
    st.divider()
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Peak Year",     str(int(df.loc[df["patents"].idxmax(), "year"])))
    col2.metric("Peak Patents",  f"{int(df['patents'].max()):,}")
    col3.metric("Avg Per Year",  f"{int(df['patents'].mean()):,}")
    col4.metric("Years Covered", str(len(df)))

    st.divider()
    st.subheader("Yearly Data Table")
    st.dataframe(df.reset_index(drop=True), use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: RECENT PATENTS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📄 Recent Patents":
    st.title("📄 Recent Chemistry Patents")
    st.caption("Patents filed from 2015 onwards")
    st.divider()

    df = data["recent_patents"].copy()

    # Filters
    col1, col2 = st.columns(2)
    with col1:
        countries = ["All"] + sorted(df["country"].dropna().unique().tolist())
        selected_country = st.selectbox("Filter by country", countries)
    with col2:
        years = ["All"] + sorted(df["year"].dropna().unique().tolist(), reverse=True)
        selected_year = st.selectbox("Filter by year", years)

    if selected_country != "All":
        df = df[df["country"] == selected_country]
    if selected_year != "All":
        df = df[df["year"] == selected_year]

    st.write(f"Showing {len(df)} patents")
    st.dataframe(
        df[["patent_id", "title", "year", "inventor", "country", "company"]]
        .reset_index(drop=True),
        use_container_width=True,
        height=500
    )