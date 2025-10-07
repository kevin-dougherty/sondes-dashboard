from __future__ import annotations
from pathlib import Path
import duckdb
import pandas as pd


SQL_CREATE = {
"launch_counts": """
CREATE TABLE IF NOT EXISTS launch_counts AS
SELECT station_id, date_trunc('hour', time) AS cycle,
COUNT(DISTINCT time) AS n_soundings
FROM parquet_scan('{parquet_root}/**/*.parquet')
GROUP BY 1, 2;
""",


"level_band_stats": """
CREATE TABLE IF NOT EXISTS level_band_stats AS
WITH levels AS (
SELECT *,
CASE
WHEN p_hPa BETWEEN 1000 AND 850 THEN '1000-850'
WHEN p_hPa BETWEEN 850 AND 500 THEN '850-500'
WHEN p_hPa BETWEEN 500 AND 200 THEN '500-200'
WHEN p_hPa < 200 THEN '<200'
ELSE 'other' END AS band
FROM parquet_scan('{parquet_root}/**/*.parquet')
)
SELECT station_id, date_trunc('day', time) AS day, band,
AVG(t_C) AS t_avg, AVG(td_C) AS td_avg,
QUANTILE_CONT(wind_spd_ms, 0.95) AS wind95
FROM levels
WHERE band <> 'other'
GROUP BY 1,2,3;
"""
}


def update_metrics(parquet_root: Path, metrics_dir: Path):
    metrics_dir.mkdir(parents=True, exist_ok=True)
    db = metrics_dir / "metrics.duckdb"
    con = duckdb.connect(str(db))
    con.execute("PRAGMA threads=4")
    for name, sql in SQL_CREATE.items():
        con.execute(sql.format(parquet_root=str(parquet_root)))
    con.close()
