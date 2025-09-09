from __future__ import annotations
import time
from pathlib import Path
from typing import Optional
import requests


HEADERS = {"User-Agent": "igra-us-ingest/0.1"}


def download_if_changed(url: str, out_path: Path, *, timeout: int = 60) -> Optional[bytes]:
    """
    Download url if remote is newer/different. Returns bytes if updated, else None.
    Uses ETag/Last-Modified caching via a local .etag sidecar.
    """

    out_path.parent.mkdir(parents=True, exist_ok=True)
    etag_path = out_path.with_suffix(out_path.suffix + ".etag")

    etag = etag_path.read_text().strip() if etag_path.exists() else None
    headers = dict(HEADERS)
    if etag:
        headers["If-None-Match"] = etag

    r = requests.get(url, headers=headers, timeout=timeout)
    if r.status_code == 304:
        return None
    r.raise_for_status()

    out_path.write_bytes(r.content)
    if et := r.headers.get("ETag"):
        etag_path.write_text(et)
    else:
        etag_path.write_text(str(time.time()))
    return r.content