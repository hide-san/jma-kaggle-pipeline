"""
JMA (Japan Meteorological Agency) API client.

Endpoints used:
- Earthquakes : https://www.jma.go.jp/bosai/quake/data/list.json
- AMeDAS temps: https://www.jma.go.jp/bosai/amedas/data/latest_time.txt
               https://www.jma.go.jp/bosai/amedas/data/map/{datetime}.json
               https://www.jma.go.jp/bosai/amedas/const/amedastable.json
- Cherry blossom: https://www.data.jma.go.jp/sakura/data/sakura{year}_obs.csv
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

    @retry(stop=stop_after_attempt(config.RETRY_ATTEMPTS),
           wait=wait_fixed(config.RETRY_WAIT_SECONDS))
    def fetch_cherry_blossom_data(self, year: int | None = None) -> pd.DataFrame:
        """Fetch cherry blossom observation data for *year* (default: current year)."""
        if year is None:
            year = datetime.now().year

        url = f"https://www.data.jma.go.jp/sakura/data/sakura{year}_obs.csv"
        log.info("Fetching cherry blossom data: %s", url)

        resp = _get(url)
        # Save raw response
        _save_raw("cherry_blossom.csv", resp.content)

        # JMA CSV uses Shift-JIS encoding
        from io import StringIO
        text = resp.content.decode("shift_jis", errors="replace")
        df = pd.read_csv(StringIO(text), skiprows=2)

        # Normalise column names
        df.columns = [c.strip() for c in df.columns]
        df.insert(0, "year", year)

        log.info("Cherry blossom rows fetched: %d", len(df))
        return df

    # ------------------------------------------------------------------ #
    # Temperature (AMeDAS)                                                 #
    # ------------------------------------------------------------------ #

    @retry(stop=stop_after_attempt(config.RETRY_ATTEMPTS),
           wait=wait_fixed(config.RETRY_WAIT_SECONDS))
    def fetch_temperature_data(self) -> pd.DataFrame:
        """Fetch latest AMeDAS surface temperature snapshot for all stations."""
        # 1. Get the latest available datetime string
        latest_url = f"{config.JMA_BASE_URL}/amedas/data/latest_time.txt"
        log.info("Fetching latest AMeDAS time: %s", latest_url)
        latest_time_str = _get(latest_url).text.strip()  # e.g. "2024-03-30T12:00:00+09:00"

        # Convert to the compact format JMA uses in map filenames: YYYYMMDD_HHMM00
        dt = datetime.fromisoformat(latest_time_str)
        dt_utc = dt.astimezone(timezone.utc)
        map_key = dt_utc.strftime("%Y%m%d_%H%M00")

        # 2. Fetch the map snapshot
        map_url = f"{config.JMA_BASE_URL}/amedas/data/map/{map_key}.json"
        log.info("Fetching AMeDAS map: %s", map_url)
        resp = _get(map_url)
        observations = resp.json()  # {station_no: {temp: [...], ...}, ...}
        # Save raw response
        _save_raw("temperatures.json", resp.content)

        # 3. Fetch station metadata (lat/lon/name) — cached once per session
        if not hasattr(self, "_amedas_table"):
            table_url = f"{config.JMA_BASE_URL}/amedas/const/amedastable.json"
            log.info("Fetching AMeDAS station table: %s", table_url)
            self._amedas_table = _get(table_url).json()

        rows = []
        for station_no, obs in observations.items():
            temp_entry = obs.get("temp")
            if temp_entry is None:
                continue
            temp_value = temp_entry[0] if isinstance(temp_entry, list) else temp_entry
            meta = self._amedas_table.get(station_no, {})
            rows.append({
                "datetime": latest_time_str,
                "station_no": station_no,
                "station_name": meta.get("kjName", ""),
                "prefecture": meta.get("pref", ""),
                "latitude": meta.get("lat", [None, None])[0],
                "longitude": meta.get("lon", [None, None])[0],
                "temperature_c": temp_value,
            })

        df = pd.DataFrame(rows)
        log.info("Temperature rows fetched: %d", len(df))
        return df

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
    path = os.path.join(config.RAW_DATA_DIR, filename)
    os.makedirs(config.RAW_DATA_DIR, exist_ok=True)
    with open(path, "wb") as f:
        f.write(content)
    log.info("Saved raw response to %s", path)
