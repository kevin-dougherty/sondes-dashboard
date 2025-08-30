"""
extract_us_stations.py

One-off utility script to generate the list of U.S. IGRA station IDs.

- Downloads the official IGRA station inventory from NCEI
- Reads it as a fixed-width file using pandas
- Filters for U.S. stations including Puerto Rico and Guam
  (IDs beginning with US, RQM, or GQM) whose end_year is 2025
  (i.e., currently active stations)
- Saves the station IDs into `../conf/stations_us.txt`

This script is not part of the dashboard pipeline; it's intended
to bootstrap the list of U.S. stations for configuration purposes.
"""

import pandas as pd

# Define the URL of the text file
url = (
    "https://www.ncei.noaa.gov/data/integrated-global-"
    "radiosonde-archive/doc/igra2-station-list.txt"
)

# Define column widths based on the text file structure
col_widths = [11, 9, 10, 7, 3, 30, 6, 5, 8]

# Define column names
col_names = [
    "stnid", "lat", "lon", "elev", "state",
    "city", "start_year", "end_year", "data"
]

# Read the fixed-width file
stnid_df = pd.read_fwf(url, names=col_names, widths=col_widths, skiprows=3)

# Filter for U.S. stations that are active through 2025
US_stnid_df = stnid_df[
    (stnid_df["stnid"].astype(str).str.startswith(("US", "RQM", "GQM")))
    & (stnid_df["end_year"] == 2025)
]["stnid"]

# Save to conf directory (relative path from scripts/)
US_stnid_df.to_csv("../conf/stations_us.txt", index=False, header=False)
