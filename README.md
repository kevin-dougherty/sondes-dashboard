# IGRA‑US Dashboard (Starter)

This repo ingests U.S. radiosonde stations from **IGRA v2.2**, parses the fixed‑width ASCII into Parquet, and precomputes tiny metrics for a dashboard. It’s built to run on a personal laptop.


## Why IGRA v2.2?
- Public, stable, daily updates.
- Station‑scoped files (period‑of‑record; year‑to‑date) keep downloads small.
- Rawinsonde data can be found here: [U.S. Rawinsonde Data](https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.ncdc:C00415)


## Sources (docs & data)
- IGRA directory index (data, derived, docs): `/pub/data/igra/`
- Sounding data (period‑of‑record): `/pub/data/igra/data/data-por/`
- Sounding data (year‑to‑date): `/pub/data/igra/data/data-y2d/`
- Data format (fixed‑width spec): `/pub/data/igra/data/igra2-data-format.txt`
- Product description (PDF): `/pub/data/igra/igra2-product-description.pdf`
- Station list (IDs, meta): `/pub/data/igra/igra2-station-list.txt`


> See links above on the NCEI site under `/pub/data/igra/`.


## Quick start
1. Install conda/mamba, then:
```bash
mamba env create -f environment.yml
mamba activate igra