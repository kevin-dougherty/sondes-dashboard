from __future__ import annotations
from pathlib import Path
from tqdm import tqdm
import zipfile
import io
import pyarrow as pa, pyarrow.parquet as pq
import pandas as pd
from .util_http import download_if_changed
from .parse_igra import parse_igra_blob


DATA_ROOT = Path("data")
POR_DIR = DATA_ROOT / "raw/por"
PARQUET_DIR = DATA_ROOT / "parquet"
BASE = "https://www.ncei.noaa.gov/pub/data/igra/data/data-por/"


STATIONS = [s.split()[0] for s in (Path("conf/stations_us.txt").read_text().splitlines()) if s.strip() and not s.strip().startswith("#")]


def write_parquet(df: pd.DataFrame):
    if df.empty:
        return
    for (stn, day), dfg in df.groupby([df.station_id, df.time.dt.strftime("%Y-%m-%d")]):
        out = PARQUET_DIR / f"station_id={stn}" / f"date={day}"
        out.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(dfg), out / "part.parquet", compression="snappy")


def main():
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    for stn in tqdm(STATIONS, desc="bootstrap POR"):
        url = f"{BASE}{stn}-data.txt.zip"
        out = POR_DIR / f"{stn}-data.txt.zip"
        blob = download_if_changed(url, out)
        if blob is None:
            blob = out.read_bytes()
        # unzip → get single .txt inside
        with zipfile.ZipFile(io.BytesIO(blob)) as zf:
            # assume exactly one file inside
            name = zf.namelist()[0]
            text = zf.read(name)
        df = parse_igra_blob(text, source_file=out.name)
        df = df.drop_duplicates(subset=["station_id", "time", "level_index"]).reset_index(drop=True)
        write_parquet(df)


if __name__ == "__main__":
    main()
