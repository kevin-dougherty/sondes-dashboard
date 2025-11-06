"""
IGRA ingest pipeline (Siphon → DuckDB)

Fetches a rolling time window of IGRA2 upper-air data for a list of stations
using Siphon, then upserts into a DuckDB table (`soundings`). Designed to be
idempotent and cron/ROCOTO friendly.

CLI:
    python -m sondes_dashboard.ingest --stations-file conf/stations_us.txt \
        --db data/igra.duckdb --end 2025101412 --days 365 --workers 8
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
from datetime import datetime, timedelta, UTC
from pathlib import Path
from typing import Iterable

import duckdb
import pandas as pd
from siphon.simplewebservice.igra2 import IGRAUpperAir
from tqdm import tqdm

DEFAULT_WINDOW_DAYS = 400 # more than one year for any overlap/backfill


def fetch_igra(
    station: str,
    start: datetime,
    end: datetime,
    synoptic_only: bool = True,
) -> pd.DataFrame:
    """
    Fetch a time range of IGRA2 data for one station via Siphon.

    Args:
        station: IGRA2 station ID (e.g., ``"USM00072201"``).
        start: Inclusive UTC start datetime.
        end: Inclusive UTC end datetime.
        synoptic_only: If True, keep only 00/12Z cycles.

    Returns:
        A DataFrame with columns subset to:
            ["station", "time", "pressure", "gph", "t", "td", "wspd", "wdir"]
        Sorted by (station, time, pressure) and deduplicated.

    Raises:
        ValueError: If ``start``/``end`` are not timezone-aware UTC.

    Notes:
        - Siphon parses and unit-normalizes the IGRA2 text under the hood.
    """
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("start/end must be timezone-aware UTC")

    df, header = IGRAUpperAir.request_data((start, end), station)
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["station"] = station

    # Normalize common columns for downstream consistency.
    rename = {
        "geopotential_height": "gph",
        "height": "gph",
        "dewpoint": "td",
        "temperature": "t",
        "wind_speed": "wspd",
        "wind_direction": "wdir",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    keep = ["station", "time", "pressure", "gph", "t", "td", "wspd", "wdir"]
    df = df[[c for c in keep if c in df.columns]]
    df = df.sort_values(["station", "time", "pressure"]).reset_index(drop=True)

    if synoptic_only and "time" in df.columns:
        df = df[df["time"].dt.hour.isin([0, 12])].reset_index(drop=True)

    # De-dup (safe for overlapping windows / repeated runs).
    df = df.drop_duplicates(subset=["station", "time", "pressure"])

    return df


def upsert_soundings(db_path: Path, df: pd.DataFrame) -> None:
    """
    Update and insert a batch of soundings into DuckDB.

    Creates the ``soundings`` table if missing, loads the provided DataFrame
    into a temporary staging table, then ``MERGE``s on the primary key
    (station, time, pressure).

    Args:
        db_path: Path to DuckDB database file.
        df: DataFrame produced by :func:`fetch_igra`.

    Returns:
        None. Data are committed to ``db_path``.

    Schema:
        station TEXT
        time TIMESTAMP WITH TIME ZONE
        pressure DOUBLE   (hPa)
        gph DOUBLE        (m)
        t DOUBLE          (°C)
        td DOUBLE         (°C)
        wspd DOUBLE       (m/s)
        wdir DOUBLE       (deg)
    """
    db = duckdb.connect(str(db_path))
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS soundings (
          station TEXT,
          time TIMESTAMP WITH TIME ZONE,
          pressure DOUBLE,
          gph DOUBLE,
          t DOUBLE,
          td DOUBLE,
          wspd DOUBLE,
          wdir DOUBLE
        );
        """
    )
    db.execute("CREATE TEMP TABLE _stage AS SELECT * FROM soundings WHERE 1=0;")
    db.register("_df", df)
    db.execute("INSERT INTO _stage SELECT * FROM _df;")
    db.execute(
        """
        MERGE INTO soundings s
        USING _stage st
          ON s.station=st.station AND s.time=st.time AND s.pressure=st.pressure
        WHEN MATCHED THEN UPDATE SET
          gph=st.gph, t=st.t, td=st.td, wspd=st.wspd, wdir=st.wdir
        WHEN NOT MATCHED THEN INSERT (station,time,pressure,gph,t,td,wspd,wdir)
          VALUES (st.station,st.time,st.pressure,st.gph,st.t,st.td,st.wspd,st.wdir);
        """
    )
    db.close()


def run(
    stations: Iterable[str],
    end: datetime,
    days: int,
    db_path: Path,
    workers: int,
    all_hours: bool,
) -> None:
    """
    Fetch and update and insert a rolling window for multiple stations concurrently.

    Args:
        stations: Iterable of IGRA2 station IDs.
        end: UTC end datetime (timezone-aware).
        days: Number of days back from ``end`` to include (rolling window).
        db_path: DuckDB database path to write/update.
        workers: Max threadpool workers (one per concurrent station).
        all_hours: If True, keep all hours; if False, filter to 00/12Z.

    Returns:
        None. Writes/updates rows in DuckDB.

    Design:
        - Parallelizes network I/O with a ThreadPool.
        - Concatenates successful station pulls and does a single upsert.
        - Logs (prints) any station failures; does not fail the whole run.
    """
    start = end - timedelta(days=days)
    pieces: list[pd.DataFrame] = []

    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_igra, s, start, end, not all_hours): s for s in stations}
        for fut in tqdm(cf.as_completed(futs), total=len(futs), desc="Fetch IGRA"):
            station = futs[fut]
            try:
                df = fut.result()
                if not df.empty:
                    pieces.append(df)
            except Exception as e:
                print(f"WARN: {station} failed: {e}")

    if not pieces:
        print("No data fetched.")
        return

    df_all = pd.concat(pieces, ignore_index=True)
    upsert_soundings(db_path, df_all)
    print(f"Ingested/updated {len(df_all)} rows into {db_path}")


def parse_args() -> argparse.Namespace:
    """
    Parse CLI arguments for the ingest module.

    CLI flags:
        --stations-file: Text file with one IGRA2 station ID per line.
        --db: Path to DuckDB file (created if missing).
        --end: UTC end datetime in ``YYYYMMDDHH`` (default: now UTC).
        --days: Rolling window length in days (default: 365).
        --workers: Threadpool size for concurrent station fetches.
        --all-hours: Include all hours (otherwise keep 00/12Z only).

    Returns:
        Parsed :class:`argparse.Namespace`.
    """
    p = argparse.ArgumentParser(description="IGRA ingest via Siphon → DuckDB")
    p.add_argument("--stations-file", required=True)
    p.add_argument("--db", default="data/igra.duckdb")
    p.add_argument("--end", help="UTC end YYYYMMDDHH; default now(UTC)")
    p.add_argument("--days", type=int, default=DEFAULT_WINDOW_DAYS)
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--all-hours", action="store_true")

    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    with open(args.stations_file) as f:
        stations = [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]
    end = (
        datetime.strptime(args.end, "%Y%m%d%H").replace(tzinfo=UTC)
        if args.end
        else datetime.now(UTC)
    )
    run(stations, end, args.days, Path(args.db), args.workers, args.all_hours)
