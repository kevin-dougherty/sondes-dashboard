"""
Sondes Dashboard (Streamlit + DuckDB)

Shows precomputed metrics from `metrics.py` for selectable windows:
7d, 30d, 3mo, 6mo, YTD, 1yr.

Run:
    streamlit run sondes_dashboard/dashboard_app.py
"""

from __future__ import annotations
import duckdb
import pandas as pd
import streamlit as st
from pathlib import Path

DB_PATH = Path(
    "/Users/kevindougherty25/python-projects/sondes-dashboard/data/igra.duckdb"
)
WINDOWS = ["7d", "30d", "3mo", "6mo", "ytd", "1yr"]

st.set_page_config(page_title="Sondes Dashboard", layout="wide")
st.title("Sondes Dashboard")

# --- Sidebar controls ---
with st.sidebar:
    st.header("Controls")
    db_file = st.text_input("DuckDB file", str(DB_PATH))
    win = st.selectbox("Window", WINDOWS, index=0)
    st.caption(
        "Tip: update metrics via your cron/ingest jobs; this app only reads DuckDB."
    )


# --- DB connection helper (reused safely) ---
@st.cache_resource(show_spinner=False)
def get_con(db_path: str):
    # read_only=True prevents accidental writes from the app
    return duckdb.connect(db_path, read_only=True)


con = get_con(db_file)


# Small helpers to cache queries per window
@st.cache_data(show_spinner=False)
def q_launches(db_path: str, win: str) -> pd.DataFrame:
    with duckdb.connect(db_path, read_only=True) as c:
        return c.execute(
            """
            SELECT cycle, stations_reporting, pct_reporting
            FROM launches_by_cycle
            WHERE window_label = ?
            ORDER BY cycle
        """,
            [win],
        ).df()


@st.cache_data(show_spinner=False)
def q_missingness(db_path: str, win: str) -> pd.DataFrame:
    with duckdb.connect(db_path, read_only=True) as c:
        return c.execute(
            """
            SELECT date, band, pct_present
            FROM missingness_by_band
            WHERE window_label = ?
            ORDER BY date, band
        """,
            [win],
        ).df()


@st.cache_data(show_spinner=False)
def q_bandstats(db_path: str, win: str) -> pd.DataFrame:
    with duckdb.connect(db_path, read_only=True) as c:
        return c.execute(
            """
            SELECT date, band,
                   t_med, t_p95, td_med, td_p95, wind_med, wind_p95
            FROM band_stats_daily
            WHERE window_label = ?
            ORDER BY date, band
        """,
            [win],
        ).df()


@st.cache_data(show_spinner=False)
def q_uptime(db_path: str, win: str) -> pd.DataFrame:
    with duckdb.connect(db_path, read_only=True) as c:
        return c.execute(
            """
            SELECT station, last_seen, days_since_last
            FROM station_uptime
            WHERE window_label = ?
            ORDER BY days_since_last, station
        """,
            [win],
        ).df()


# --- Top KPIs (quick read) ---
kcol1, kcol2, kcol3 = st.columns(3)
try:
    launches = q_launches(db_file, win)
    if not launches.empty:
        latest = launches.iloc[-1]
        kcol1.metric(
            "Stations reporting (last cycle)", int(latest["stations_reporting"])
        )
        kcol2.metric("% Reporting (last cycle)", f"{latest['pct_reporting']:.1f}%")
        kcol3.metric("Cycles in window", launches["cycle"].nunique())
except Exception as e:
    st.warning(f"Launch KPI error: {e}")

st.divider()

# --- Launches per cycle & % reporting ---
col1, col2 = st.columns(2)
with col1:
    st.subheader("Launches per 00/12Z cycle")
    if launches.empty:
        st.info("No data for this window.")
    else:
        st.line_chart(launches.set_index("cycle")["stations_reporting"])
with col2:
    st.subheader("Percent of stations reporting")
    if launches.empty:
        st.info("No data for this window.")
    else:
        st.line_chart(launches.set_index("cycle")["pct_reporting"])

st.divider()

# --- Missingness by band ---
st.subheader("Presence by pressure band (% of possible station×cycle pairs)")
missing = q_missingness(db_file, win)
if missing.empty:
    st.info("No missingness data for this window.")
else:
    bands = missing["band"].unique().tolist()
    sel_bands = st.multiselect("Bands", bands, default=bands, key=f"bands_{win}")
    if sel_bands:
        miss_pivot = (
            missing[missing["band"].isin(sel_bands)]
            .pivot(index="date", columns="band", values="pct_present")
            .sort_index()
        )
        st.line_chart(miss_pivot)
    else:
        st.info("Select at least one band.")

st.divider()

# --- Band stats (median/p95) ---
st.subheader("Band statistics (daily)")
stats = q_bandstats(db_file, win)
if stats.empty:
    st.info("No band statistics for this window.")
else:
    metric = st.selectbox(
        "Metric", ["Temperature (°C)", "Dewpoint (°C)", "Wind speed (m/s)"], index=0
    )
    if metric.startswith("Temp"):
        med_col, p95_col = "t_med", "t_p95"
    elif metric.startswith("Dew"):
        med_col, p95_col = "td_med", "td_p95"
    else:
        med_col, p95_col = "wind_med", "wind_p95"

    bands2 = stats["band"].unique().tolist()
    sel_bands2 = st.multiselect(
        "Bands (stats)", bands2, default=bands2, key=f"bands_stats_{win}"
    )

    if sel_bands2:
        med = (
            stats[stats["band"].isin(sel_bands2)]
            .pivot(index="date", columns="band", values=med_col)
            .sort_index()
        )
        p95 = (
            stats[stats["band"].isin(sel_bands2)]
            .pivot(index="date", columns="band", values=p95_col)
            .sort_index()
        )
        st.write("Median")
        st.line_chart(med)
        st.write("95th percentile")
        st.line_chart(p95)
    else:
        st.info("Select at least one band.")

st.divider()

# --- Station uptime table ---
st.subheader("Station uptime")
uptime = q_uptime(db_file, win)
if uptime.empty:
    st.info("No uptime data for this window.")
else:
    # Optional filter
    prefix = st.text_input("Filter stations by prefix (e.g., 'USM000')", "")
    if prefix:
        uptime_view = uptime[uptime["station"].str.startswith(prefix)]
    else:
        uptime_view = uptime
    st.dataframe(uptime_view, use_container_width=True)
