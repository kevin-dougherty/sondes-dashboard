from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator
import gzip, io
import pandas as pd


# === IMPORTANT ===
# Replace the placeholder slices below with exact columns from the IGRA spec:
# https://www.ncei.noaa.gov/pub/data/igra/data/igra2-data-format.txt
# Keep both header and level line formats here for clarity.


HEADER_SLICE = {
    "station_id": (0, 11), # chars 1-11
    "yyyymmddhh": (12, 22), # adjust after reading spec
    "n_levels": (23, 27), # integer count
    }


LEVEL_SLICE = {
    "p_hPa": (0, 7),
    "z_m": (7, 13),
    "t_C": (13, 19),
    "td_dep_C": (19, 25), # dewpoint depression; Td = T - dep
    "wind_dir_deg": (25, 29),
    "wind_spd_ms": (29, 34),
    # QC flags (single-character codes per variable)
    "qc_p": (60, 61),
    "qc_z": (61, 62),
    "qc_t": (62, 63),
    "qc_td": (63, 64),
    "qc_w": (64, 65),
    }


@dataclass
class Level:
    p_hPa: float | None
    z_m: float | None
    t_C: float | None
    td_C: float | None
    wind_dir_deg: float | None
    wind_spd_ms: float | None
    qc_p: str | None
    qc_z: str | None
    qc_t: str | None
    qc_td: str | None
    qc_w: str | None


def _to_float(s: str) -> float | None:
    s = s.strip()
    if not s or s in {"-9999", "-999.9", "-99.9"}:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _slice(line: str, start: int, end: int) -> str:
    return line[start:end]


def _iter_soundings(lines: Iterable[str]) -> Iterator[tuple[str, str, list[Level]]]:
    it = iter(lines)
    for line in it:
        if not line.strip():
            continue

        stn = _slice(line, *HEADER_SLICE["station_id"]).strip()
        ts = _slice(line, *HEADER_SLICE["yyyymmddhh"]).strip()
        nlev_str = _slice(line, *HEADER_SLICE["n_levels"]).strip()
        if not stn or not ts or not nlev_str.isdigit():
            continue

        nlev = int(nlev_str)
        levels: list[Level] = []
        for _ in range(nlev):
            lvl = next(it)
            p = _to_float(_slice(lvl, *LEVEL_SLICE["p_hPa"]))
            z = _to_float(_slice(lvl, *LEVEL_SLICE["z_m"]))
            t = _to_float(_slice(lvl, *LEVEL_SLICE["t_C"]))
            dep = _to_float(_slice(lvl, *LEVEL_SLICE["td_dep_C"]))
            td = (t - dep) if (t is not None and dep is not None) else None
            wd = _to_float(_slice(lvl, *LEVEL_SLICE["wind_dir_deg"]))
            ws = _to_float(_slice(lvl, *LEVEL_SLICE["wind_spd_ms"]))
            qc_p = _slice(lvl, *LEVEL_SLICE["qc_p"]).strip() or None
            qc_z = _slice(lvl, *LEVEL_SLICE["qc_z"]).strip() or None
            qc_t = _slice(lvl, *LEVEL_SLICE["qc_t"]).strip() or None
            qc_td = _slice(lvl, *LEVEL_SLICE["qc_td"]).strip() or None
            qc_w = _slice(lvl, *LEVEL_SLICE["qc_w"]).strip() or None
            levels.append(Level(p, z, t, td, wd, ws, qc_p, qc_z, qc_t, qc_td, qc_w))

        yield stn, ts, levels

def parse_igra_blob(blob: bytes, source_file: str) -> pd.DataFrame:
    """
    Parse a .txt or .zip/.gz payload into a tidy DataFrame.
    The caller should convert station time strings to timezone-aware UTC.
    """
    # Handle gzip transparently
    try:
        text = gzip.open(io.BytesIO(blob), "rt", encoding="utf-8", errors="ignore").read()
    except OSError:
        text = blob.decode("utf-8", errors="ignore")
    
    
    rows = []
    for stn, ts, levels in _iter_soundings(text.splitlines()):
        for i, lv in enumerate(levels):
            rows.append({
            "station_id": stn,
            "time": ts,
            "level_index": i,
            "p_hPa": lv.p_hPa,
            "z_m": lv.z_m,
            "t_C": lv.t_C,
            "td_C": lv.td_C,
            "wind_dir_deg": lv.wind_dir_deg,
            "wind_spd_ms": lv.wind_spd_ms,
            "qc_p": lv.qc_p,
            "qc_z": lv.qc_z,
            "qc_t": lv.qc_t,
            "qc_td": lv.qc_td,
            "qc_w": lv.qc_w,
            "source_file": source_file,
            })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["time"] = pd.to_datetime(df["time"], format="%Y%m%d%H", utc=True)
    return df