"""Shared utilities for JMA API client."""

import os

import requests

import config
from logger import get_logger

log = get_logger(__name__)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "jma-kaggle-pipeline/1.0"})


def get(url: str, **kwargs) -> requests.Response:
    """Make HTTP GET request with session and error handling."""
    resp = SESSION.get(url, timeout=30, **kwargs)
    resp.raise_for_status()
    return resp


def is_numeric(s: str) -> bool:
    """Check if string can be converted to float."""
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False


def parse_latlon(cod: str) -> tuple[float | None, float | None]:
    """Parse 'lat lon' string like '+35.6 +139.7' into floats."""
    parts = cod.strip().split()
    if len(parts) == 2:
        try:
            return float(parts[0]), float(parts[1])
        except ValueError:
            pass
    return None, None


def save_raw(filename: str, content: bytes) -> None:
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
