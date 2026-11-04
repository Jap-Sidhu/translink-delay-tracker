"""Read route names from TransLink's static GTFS zip."""

from __future__ import annotations

import csv
import io
import zipfile

import requests

from .config import STATIC_GTFS_URL, USER_AGENT


def parse_routes(zip_bytes: bytes) -> list[dict]:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        with archive.open("routes.txt") as raw:
            reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig"))
            return [
                {
                    "route_id": row["route_id"].strip(),
                    "short_name": (row.get("route_short_name") or "").strip() or None,
                    "long_name": (row.get("route_long_name") or "").strip() or None,
                    "route_type": int(row["route_type"]) if row.get("route_type") else None,
                }
                for row in reader
                if row.get("route_id", "").strip()
            ]


def download_routes(session: requests.Session, last_modified: str | None) -> tuple[list[dict] | None, str | None]:
    """Return (routes, last_modified), with routes None when the file is unchanged."""
    headers = {"User-Agent": USER_AGENT}
    if last_modified:
        headers["If-Modified-Since"] = last_modified
    response = session.get(STATIC_GTFS_URL, headers=headers, timeout=(10, 120))
    if response.status_code == 304:
        return None, last_modified
    response.raise_for_status()
    return parse_routes(response.content), response.headers.get("Last-Modified")
