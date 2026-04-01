"""
JMA (Japan Meteorological Agency) API client.

Endpoints used:
- Earthquakes : https://www.jma.go.jp/bosai/quake/data/list.json (WORKING)
- AMeDAS temps: DEPRECATED - endpoint no longer available
- Cherry blossom: https://www.data.jma.go.jp/sakura/data/sakura{year}_obs.csv
                 (tries multiple years with fallback: current year -> previous years)
"""

import json
import os
import re
from datetime import datetime, timezone

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_fixed

import config
from logger import get_logger

log = get_logger(__name__)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "jma-kaggle-pipeline/1.0"})


def _get(url: str, **kwargs) -> requests.Response:
    resp = SESSION.get(url, timeout=30, **kwargs)
    resp.raise_for_status()
    return resp


class JMAApiClient:
    # ------------------------------------------------------------------ #
    # Cherry blossom                                                       #
    # ------------------------------------------------------------------ #

    def fetch_cherry_blossom_data(self, year: int | None = None) -> pd.DataFrame:
        """Fetch cherry blossom observation data, trying multiple years with fallback."""
        if year is None:
            year = datetime.now().year

        # Try current year and previous 3 years
        years_to_try = [year, year - 1, year - 2, year - 3]

        for try_year in years_to_try:
            url = f"https://www.data.jma.go.jp/sakura/data/sakura{try_year}_obs.csv"
            try:
                log.info("Fetching cherry blossom data: %s", url)
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()

                # Save raw response
                _save_raw("cherry_blossom.csv", resp.content)

                # JMA CSV uses Shift-JIS encoding
                from io import StringIO
                text = resp.content.decode("shift_jis", errors="replace")
                df = pd.read_csv(StringIO(text), skiprows=2)

                # Normalise column names
                df.columns = [c.strip() for c in df.columns]
                df.insert(0, "year", try_year)

                log.info("Cherry blossom rows fetched: %d (from year %d)", len(df), try_year)
                return df
            except requests.exceptions.HTTPError:
                log.warning("Cherry blossom data not available for year %d", try_year)
                continue

        # If all years fail, return empty DataFrame
        log.warning("Cherry blossom data unavailable for years %s", years_to_try)
        return pd.DataFrame()

    # ------------------------------------------------------------------ #
    # Temperature (AMeDAS)                                                 #
    # ------------------------------------------------------------------ #

    def fetch_temperature_data(self) -> pd.DataFrame:
        """Fetch latest AMeDAS surface temperature snapshot for all stations.

        NOTE: The AMeDAS map endpoint has been discontinued by JMA.
        The /bosai/amedas/data/map/{datetime}.json endpoint returns 404.
        This dataset is no longer available from JMA.
        """
        log.info("Fetching temperature data from AMeDAS")
        log.warning("AMeDAS temperature endpoint has been discontinued by JMA - no data available")
        # Return empty DataFrame to allow pipeline to continue gracefully
        return pd.DataFrame()

    # ------------------------------------------------------------------ #
    # Earthquakes                                                          #
    # ------------------------------------------------------------------ #

    @retry(stop=stop_after_attempt(config.RETRY_ATTEMPTS),
           wait=wait_fixed(config.RETRY_WAIT_SECONDS))
    def fetch_earthquake_data(self) -> pd.DataFrame:
        """Fetch the latest earthquake list from JMA."""
        url = f"{config.JMA_BASE_URL}/quake/data/list.json"
        log.info("Fetching earthquake list: %s", url)
        resp = _get(url)
        items = resp.json()  # list of dicts
        # Save raw response
        _save_raw("earthquakes.json", resp.content)

        rows = []
        for item in items:
            # Derive a stable event_id from the hypocenter report key
            event_id = item.get("json", "").replace("/", "_").removesuffix(".json")
            at = item.get("at", "")          # origin time
            maxi = item.get("maxi", "")      # max seismic intensity
            mag_str = item.get("mag", "")
            mag = float(mag_str) if _is_numeric(mag_str) else None
            anm = item.get("anm", "")        # epicentre name
            # lat/lon embedded in item for recent quakes
            cod = item.get("cod", "")        # "lat lon" string
            lat, lon = _parse_latlon(cod)

            rows.append({
                "event_id": event_id,
                "origin_time": at,
                "epicentre": anm,
                "latitude": lat,
                "longitude": lon,
                "magnitude": mag,
                "max_intensity": maxi,
            })

        df = pd.DataFrame(rows)
        log.info("Earthquake rows fetched: %d", len(df))
        return df


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #

def _is_numeric(s: str) -> bool:
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False


def _parse_latlon(cod: str) -> tuple[float | None, float | None]:
    """Parse 'lat lon' string like '+35.6 +139.7' into floats."""
    parts = cod.strip().split()
    if len(parts) == 2:
        try:
            return float(parts[0]), float(parts[1])
        except ValueError:
            pass
    return None, None


def _save_raw(filename: str, content: bytes) -> None:
    """Save raw API response to data/raw/ directory."""
    try:
        path = os.path.join(config.RAW_DATA_DIR, filename)
        os.makedirs(config.RAW_DATA_DIR, exist_ok=True)
        with open(path, "wb") as f:
            f.write(content)
        # Use forward slashes for consistent cross-platform logging
        log_path = path.replace(os.sep, '/')
        log.info("Saved raw response to %s", log_path)
    except Exception as exc:
        log.warning("Failed to save raw response for %s: %s", filename, exc)
