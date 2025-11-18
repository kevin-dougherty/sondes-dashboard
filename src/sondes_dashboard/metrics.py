"""
Metric computation for Sondes Dashboard (DuckDB → metric tables)

Reads base `soundings` from DuckDB and computes small, fast, denormalized
tables for the dashboard:
  - launches_by_cycle      (counts and % stations reporting at 00/12Z)
  - missingness_by_band    (% present by pressure band, daily)
  - band_stats_daily       (median / p95 for T, Td, wind by band, daily)
  - station_uptime         (last seen and staleness, per station)
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

WINDOWS = {
    "7d": 7,
    "30d": 30,
    "3mo": 90,
    "6mo": 180,
    "1yr": 365,
    # ytd handled specially below
}


def ytd_days(end_dt):
    """
    Calculates number of days to Jan 1 for Y2D stats.
    """
    jan1 = end_dt.replace(month=1, day=1, hour=0)
    return (end_dt - jan1).days + 1


def compute_and_store_metrics(
    db_path: Path, end: datetime, windows: list[str] | None = None
):
    """
    Compute dashboard metrics for multiple windows and update and insert into DuckDB.
    Creates/updates:
      launches_by_cycle(window, cycle, stations_reporting, pct_reporting)
      missingness_by_band(window, date, band, pct_present)
      band_stats_daily(window, date, band, t_med, t_p95, td_med, td_p95, wind_med, wind_p95)
      station_uptime(window, station, last_seen, days_since_last)
    """
    if windows is None:
        windows = ["7d", "30d", "3mo", "6mo", "ytd", "1yr"]

    db = duckdb.connect(str(db_path))
    db.execute("SET TimeZone='UTC';")

    # Create schemas once (now with `window`)
    db.execute(
    """
    CREATE TABLE IF NOT EXISTS launches_by_cycle (
      "window" TEXT,
      cycle TIMESTAMPTZ,
      stations_reporting INTEGER,
      pct_reporting DOUBLE
    );
    CREATE TABLE IF NOT EXISTS missingness_by_band (
      "window" TEXT,
      date DATE,
      band TEXT,
      pct_present DOUBLE
    );
    CREATE TABLE IF NOT EXISTS band_stats_daily (
      "window" TEXT,
      date DATE,
      band TEXT,
      -- temperature
      t_q25 DOUBLE,
      t_med DOUBLE,
      t_q75 DOUBLE,
      t_iqr DOUBLE,
      t_p95 DOUBLE,
      -- dewpoint
      td_q25 DOUBLE,
      td_med DOUBLE,
      td_q75 DOUBLE,
      td_iqr DOUBLE,
      td_p95 DOUBLE,
      -- wind
      wind_q25 DOUBLE,
      wind_med DOUBLE,
      wind_q75 DOUBLE,
      wind_iqr DOUBLE,
      wind_p95 DOUBLE
    );
    CREATE TABLE IF NOT EXISTS station_uptime (
      "window" TEXT,
      station TEXT,
      last_seen TIMESTAMPTZ,
      days_since_last DOUBLE
    );
    -- per-station, per-cycle status within a window
    CREATE TABLE IF NOT EXISTS station_cycle_status (
      "window" TEXT,
      cycle TIMESTAMPTZ,
      station TEXT,
      reported BOOLEAN
    );
    -- per-station, per-window reporting efficiency
    CREATE TABLE IF NOT EXISTS station_reporting_stats (
      "window" TEXT,
      station TEXT,
      total_cycles INTEGER,
      reported_cycles INTEGER,
      pct_reporting DOUBLE
    );
    """
    )

    for w in windows:
        days = ytd_days(end) if w == "ytd" else WINDOWS[w]
        start = end - timedelta(days=days)
    
        # Format timestamps for DuckDB (UTC, no timezone in literal)
        start_ts = start.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
        end_ts   = end.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
    
        # Rebuild a view limited to this window
        db.execute("DROP VIEW IF EXISTS _v;")
        db.execute(
            f"""
            CREATE VIEW _v AS
            SELECT *
            FROM soundings
            WHERE time BETWEEN TIMESTAMP '{start_ts}'
                          AND TIMESTAMP '{end_ts}';
            """
        )

        # Clean the overlapping rows for this window label only
        db.execute('DELETE FROM launches_by_cycle WHERE "window" = ?;', [w])
        db.execute('DELETE FROM missingness_by_band WHERE "window" = ?;', [w])
        db.execute('DELETE FROM band_stats_daily WHERE "window" = ?;', [w])
        db.execute('DELETE FROM station_uptime WHERE "window" = ?;', [w])
        db.execute('DELETE FROM station_cycle_status WHERE "window" = ?;', [w])
        db.execute('DELETE FROM station_reporting_stats WHERE "window" = ?;', [w])

        # 0) Station × cycle reporting grid (for maps and per-station stats)
        db.execute(
            """
            INSERT INTO station_cycle_status
            WITH cycles AS (
              SELECT DISTINCT date_trunc('hour', time) AS cycle
              FROM _v
              WHERE EXTRACT(hour FROM time) IN (0, 12)
            ),
            stations AS (
              SELECT DISTINCT station
              FROM _v
            ),
            grid AS (
              SELECT c.cycle, s.station
              FROM cycles c
              CROSS JOIN stations s
            ),
            reported AS (
              SELECT
                station,
                date_trunc('hour', time) AS cycle
              FROM _v
              WHERE EXTRACT(hour FROM time) IN (0, 12)
              GROUP BY station, cycle
            )
            SELECT
              ? AS "window",
              g.cycle,
              g.station,
              (r.station IS NOT NULL) AS reported
            FROM grid g
            LEFT JOIN reported r
              ON g.station = r.station
             AND g.cycle   = r.cycle
            ORDER BY g.cycle, g.station;
            """,
            [w],
        )
        
        # 1) Launches per 00/12Z (via station_cycle_status)
        db.execute(
            """
            INSERT INTO launches_by_cycle
            WITH counts AS (
              SELECT
                cycle,
                COUNT_IF(reported) AS stations_reporting
              FROM station_cycle_status
              WHERE "window" = ?
              GROUP BY cycle
            ),
            denom AS (
              SELECT COUNT(DISTINCT station) AS n
              FROM station_cycle_status
              WHERE "window" = ?
            )
            SELECT
              ? AS "window",
              c.cycle,
              c.stations_reporting,
              100.0 * c.stations_reporting / NULLIF(d.n, 0) AS pct_reporting
            FROM counts c, denom d
            ORDER BY c.cycle;
            """,
            [w, w, w],
        )

        # 2) Missingness by band
        db.execute("DROP TABLE IF EXISTS _banded;")
        # First: create the temp table (no parameters here)
        db.execute(
            """
            CREATE TEMP TABLE _banded AS
            SELECT *,
                   CASE
                     WHEN pressure BETWEEN 850 AND 1000 THEN '1000_850'
                     WHEN pressure BETWEEN 500 AND  850 THEN '850_500'
                     WHEN pressure BETWEEN 200 AND  500 THEN '500_200'
                     WHEN pressure < 200 THEN 'lt_200'
                   END AS band
            FROM _v;
            """
        )

        # Second: insert into missingness_by_band with the window parameter
        db.execute(
            """
            INSERT INTO missingness_by_band
            WITH base AS (
              SELECT station, date_trunc('hour', time) AS cycle, band
              FROM _banded
              WHERE band IS NOT NULL AND EXTRACT(hour FROM time) IN (0,12)
              GROUP BY station, cycle, band
            ),
            per_cycle AS (
              SELECT date_trunc('day', cycle) AS date, band, COUNT(*) AS present_pairs
              FROM base
              GROUP BY date, band
            ),
            denom AS (
              SELECT d.date, COUNT(DISTINCT s.station) * 2 AS denom
              FROM (SELECT DISTINCT date_trunc('day', time) AS date FROM _v) d
              LEFT JOIN (SELECT DISTINCT station FROM _v) s ON TRUE
              GROUP BY d.date
            )
            SELECT
              ? AS "window",
              p.date,
              p.band,
              100.0 * p.present_pairs / NULLIF(d.denom, 0) AS pct_present
            FROM per_cycle p
            JOIN denom d USING (date)
            ORDER BY p.date, p.band;
            """,
            [w],
        )

        # 3) Band stats daily (now with quartiles + IQR)
        db.execute(
            """
            INSERT INTO band_stats_daily
            WITH daily AS (
              SELECT date_trunc('day', time) AS date, band,
                     t    AS T,
                     td   AS Td,
                     wspd AS Wind
              FROM _banded
              WHERE band IS NOT NULL
            )
            SELECT
              ? AS "window",
              date,
              band,
        
              -- Temperature
              quantile_cont(T, 0.25) AS t_q25,
              quantile_cont(T, 0.50) AS t_med,
              quantile_cont(T, 0.75) AS t_q75,
              quantile_cont(T, 0.75) - quantile_cont(T, 0.25) AS t_iqr,
              quantile_cont(T, 0.95) AS t_p95,
        
              -- Dewpoint
              quantile_cont(Td, 0.25) AS td_q25,
              quantile_cont(Td, 0.50) AS td_med,
              quantile_cont(Td, 0.75) AS td_q75,
              quantile_cont(Td, 0.75) - quantile_cont(Td, 0.25) AS td_iqr,
              quantile_cont(Td, 0.95) AS td_p95,
        
              -- Wind
              quantile_cont(Wind, 0.25) AS wind_q25,
              quantile_cont(Wind, 0.50) AS wind_med,
              quantile_cont(Wind, 0.75) AS wind_q75,
              quantile_cont(Wind, 0.75) - quantile_cont(Wind, 0.25) AS wind_iqr,
              quantile_cont(Wind, 0.95) AS wind_p95
        
            FROM daily
            GROUP BY date, band
            ORDER BY date, band;
            """,
            [w],
        )

        # 4) Station uptime
        db.execute(
            """
        INSERT INTO station_uptime
        SELECT ? AS "window", station,
               MAX(time) AS last_seen,
               DATE_DIFF('day', MAX(time), CURRENT_TIMESTAMP) AS days_since_last
        FROM _v
        GROUP BY station
        ORDER BY station;
        """,
            [w],
        )

        # 5) Station reporting efficiency over the window
        db.execute(
            """
            INSERT INTO station_reporting_stats
            SELECT
              ?      AS "window",
              station,
              COUNT(*)                          AS total_cycles,
              COUNT_IF(reported)                AS reported_cycles,
              100.0 * COUNT_IF(reported)
                     / NULLIF(COUNT(*), 0)      AS pct_reporting
            FROM station_cycle_status
            WHERE "window" = ?
            GROUP BY station
            ORDER BY station;
            """,
            [w, w],
        )

    db.close()
    print(f"Metrics updated for windows: {', '.join(windows)}")


def parse_args() -> argparse.Namespace:
    """
    Parse CLI arguments for the metrics module.

    CLI flags:
        --db: DuckDB database path.
        --end: UTC end datetime in ``YYYYMMDDHH`` (default: now UTC).
        --days: Rolling window length to include for metrics.

    Returns:
        Parsed :class:`argparse.Namespace`.
    """
    ap = argparse.ArgumentParser(description="Compute dashboard metrics from DuckDB")
    ap.add_argument("--db", default="data/igra.duckdb")
    ap.add_argument("--end", help="UTC end YYYYMMDDHH; default now(UTC)")
    ap.add_argument("--days", type=int, default=365)
    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args()
    end = (
        datetime.strptime(args.end, "%Y%m%d%H").replace(tzinfo=UTC)
        if args.end
        else datetime.now(UTC)
    )
    compute_and_store_metrics(Path(args.db), end=end)
