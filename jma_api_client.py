"""
JMA (Japan Meteorological Agency) API client.

Endpoints:
- Earthquake list (JSON): https://www.jma.go.jp/bosai/quake/data/list.json
  Simple list of recent earthquakes with basic info (magnitude, intensity, location)

- JMA Data Feeds (XML Atom):
  - regular_l.xml: Regular information (3.1MB, weather, forecasts, etc.)
  - extra_l.xml: Extra/additional information (1.5MB)
  - eqvol_l.xml: Earthquake & Volcano information (387KB) - Primary source for earthquake data
  - other_l.xml: Other information (245KB)

  Each feed contains Atom entries with links to detailed meteorological and seismic data files.
"""

import json
import os
import re
import xml.etree.ElementTree as ET
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
        """Fetch earthquake data from JMA APIs.

        Fetches all 4 JMA Data Feeds and prioritizes eqvol_l.xml for earthquake data.
        Also uses the simple JSON endpoint as fallback.
        """
        # JMA Data Feed URLs
        feeds = [
            ("regular_l.xml", "https://www.data.jma.go.jp/developer/xml/feed/regular_l.xml"),
            ("extra_l.xml", "https://www.data.jma.go.jp/developer/xml/feed/extra_l.xml"),
            ("eqvol_l.xml", "https://www.data.jma.go.jp/developer/xml/feed/eqvol_l.xml"),
            ("other_l.xml", "https://www.data.jma.go.jp/developer/xml/feed/other_l.xml"),
        ]

        # Fetch and save all JMA Data Feeds
        for feed_name, feed_url in feeds:
            try:
                log.info("Fetching %s", feed_name)
                feed_resp = _get(feed_url)
                _save_raw(feed_name, feed_resp.content)
                log.info("Saved %s (%d bytes)", feed_name, len(feed_resp.content))
            except Exception as exc:
                log.warning("Could not fetch %s: %s", feed_name, exc)

        # Fetch earthquake list from simple JSON endpoint
        url = "https://www.jma.go.jp/bosai/quake/data/list.json"
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

    @retry(stop=stop_after_attempt(config.RETRY_ATTEMPTS),
           wait=wait_fixed(config.RETRY_WAIT_SECONDS))
    def fetch_earthquakes_enhanced(self) -> pd.DataFrame:
        """Fetch enhanced earthquake data from JMA XML feed (eqvol_l.xml).

        Parses VXSE53 entries (earthquake/seismic) with detailed hypocenter,
        magnitude type, and per-prefecture seismic intensity information.

        Enhanced fields vs. simple JSON:
        - hypocenter_latitude, hypocenter_longitude, hypocenter_depth_km
        - magnitude_type (usually 'Mj')
        - per_prefecture_intensity (JSON dict with prefecture names/codes and intensities)
        """
        # Read locally saved eqvol_l.xml feed
        feed_path = os.path.join(config.RAW_DATA_DIR, "eqvol_l.xml")

        if not os.path.exists(feed_path):
            log.warning("eqvol_l.xml not found in %s, skipping enhanced earthquakes", config.RAW_DATA_DIR)
            return pd.DataFrame()

        try:
            with open(feed_path, 'rb') as f:
                feed_content = f.read()
        except Exception as exc:
            log.error("Could not read eqvol_l.xml: %s", exc)
            return pd.DataFrame()

        # Parse Atom feed
        try:
            root = ET.fromstring(feed_content)
        except ET.ParseError as exc:
            log.error("Could not parse eqvol_l.xml: %s", exc)
            return pd.DataFrame()

        ns = {'atom': 'http://www.w3.org/2005/Atom'}
        entries = root.findall('.//atom:entry', ns)
        log.info("Found %d entries in eqvol_l.xml", len(entries))

        rows = []
        entry_count = 0

        # Process each feed entry
        for entry in entries:
            try:
                link = entry.find('atom:link', ns)
                if link is None:
                    continue

                data_url = link.get('href')
                if not data_url or 'VXSE5' not in data_url:  # Only process earthquake entries
                    continue

                entry_count += 1
                if entry_count > 50:  # Limit to recent 50 earthquakes
                    break

                # Fetch and parse the detailed XML data file
                try:
                    data_resp = _get(data_url)
                    data_root = ET.fromstring(data_resp.content)
                    earthquake_data = self._parse_earthquake_xml(data_root, data_url)
                    if earthquake_data:
                        rows.append(earthquake_data)
                except Exception as exc:
                    log.debug("Failed to parse earthquake XML from %s: %s", data_url, exc)
                    continue

            except Exception as exc:
                log.warning("Failed to process entry: %s", exc)
                continue

        df = pd.DataFrame(rows)
        log.info("Enhanced earthquake rows fetched: %d", len(df))
        return df

    def _parse_earthquake_xml(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VXSE53 earthquake XML and extract rich earthquake data."""
        def sn(tag):
            """Strip namespace from tag."""
            return tag.split('}')[-1] if '}' in tag else tag

        # Find report metadata in Head (try multiple namespace variants)
        head = root.find('.//{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Head')
        if head is None:
            head = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Head')

        # Find body (try seismology1 namespace first)
        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/seismology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if head is None or body is None:
            return None

        # Extract header info
        event_id = None
        report_datetime = None

        for elem in head.iter():
            tag = sn(elem.tag)
            if tag == 'EventID' and elem.text:
                event_id = elem.text
            elif tag == 'ReportDateTime' and elem.text:
                report_datetime = elem.text

        if not event_id:
            return None  # Not an earthquake event

        # Extract Earthquake data from Body
        eq_data = {'event_id': event_id, 'report_datetime': report_datetime}

        # Find Earthquake element
        for elem in body.iter():
            tag = sn(elem.tag)

            if tag == 'OriginTime' and elem.text:
                eq_data['origin_time'] = elem.text

            elif tag == 'Hypocenter':
                # Parse hypocenter (Area/Coordinate structure)
                for area_elem in elem:
                    if sn(area_elem.tag) == 'Area':
                        for area_child in area_elem:
                            child_tag = sn(area_child.tag)
                            if child_tag == 'Name' and area_child.text:
                                eq_data['hypocenter_area'] = area_child.text
                            elif child_tag == 'Coordinate' and area_child.text:
                                # Parse "+lat+lon-depth/" format (coordinates without spaces)
                                coord_str = area_child.text.strip().replace('/', '')
                                try:
                                    # Use regex to extract signed floats
                                    coords = re.findall(r'[+-]?\d+\.?\d*', coord_str)
                                    if len(coords) >= 2:
                                        eq_data['hypocenter_latitude'] = float(coords[0])
                                        eq_data['hypocenter_longitude'] = float(coords[1])
                                        if len(coords) >= 3:
                                            # Depth is in meters, convert to km
                                            # Take absolute value in case negative depth is used
                                            depth_m = abs(float(coords[2]))
                                            eq_data['hypocenter_depth_km'] = depth_m / 1000
                                except (ValueError, IndexError):
                                    pass

            elif tag == 'Magnitude':
                # Magnitude value is in text content, type is in attribute
                if elem.text:
                    try:
                        eq_data['magnitude'] = float(elem.text)
                    except ValueError:
                        pass
                # Get magnitude type from attributes
                mag_type = elem.get('type')
                if mag_type:
                    eq_data['magnitude_type'] = mag_type

            elif tag == 'MaxInt' and elem.text:
                eq_data['max_intensity'] = elem.text

        # Extract per-prefecture intensity data
        prefectures = {}
        for elem in body.iter():
            tag = sn(elem.tag)
            if tag == 'Pref':
                pref_name = None
                pref_intensity = None
                for child in elem:
                    child_tag = sn(child.tag)
                    if child_tag == 'Name' and child.text:
                        pref_name = child.text
                    elif child_tag == 'MaxInt' and child.text:
                        pref_intensity = child.text
                if pref_name and pref_intensity:
                    prefectures[pref_name] = pref_intensity

        if prefectures:
            eq_data['prefectures_intensity_json'] = json.dumps(prefectures, ensure_ascii=False)

        return eq_data if len(eq_data) > 1 else None  # Return only if has data beyond event_id

    # ------------------------------------------------------------------ #
    # Volcanoes                                                            #
    # ------------------------------------------------------------------ #

    @retry(stop=stop_after_attempt(config.RETRY_ATTEMPTS),
           wait=wait_fixed(config.RETRY_WAIT_SECONDS))
    def fetch_volcanic_ash_forecasts(self) -> pd.DataFrame:
        """Fetch volcanic ash forecasts from JMA XML feed (eqvol_l.xml).

        Parses VFVO53 entries (volcanic ash) with 6 time-window forecasts per event.

        Enhanced fields per forecast:
        - event_id, report_datetime, volcano_name
        - For each 6 windows: window_N_start, window_N_end, window_N_areas
        """
        # Read locally saved eqvol_l.xml feed
        feed_path = os.path.join(config.RAW_DATA_DIR, "eqvol_l.xml")

        if not os.path.exists(feed_path):
            log.warning("eqvol_l.xml not found in %s, skipping volcanic ash forecasts", config.RAW_DATA_DIR)
            return pd.DataFrame()

        try:
            with open(feed_path, 'rb') as f:
                feed_content = f.read()
        except Exception as exc:
            log.error("Could not read eqvol_l.xml: %s", exc)
            return pd.DataFrame()

        # Parse Atom feed
        try:
            root = ET.fromstring(feed_content)
        except ET.ParseError as exc:
            log.error("Could not parse eqvol_l.xml: %s", exc)
            return pd.DataFrame()

        ns = {'atom': 'http://www.w3.org/2005/Atom'}
        entries = root.findall('.//atom:entry', ns)
        log.info("Found %d entries in eqvol_l.xml", len(entries))

        rows = []
        entry_count = 0

        # Process each feed entry
        for entry in entries:
            try:
                link = entry.find('atom:link', ns)
                if link is None:
                    continue

                data_url = link.get('href')
                if not data_url or 'VFVO5' not in data_url:  # Only process volcanic ash entries
                    continue

                entry_count += 1
                if entry_count > 100:  # Limit to recent 100 volcanic ash forecasts
                    break

                # Fetch and parse the detailed XML data file
                try:
                    data_resp = _get(data_url)
                    data_root = ET.fromstring(data_resp.content)
                    volcano_data = self._parse_volcanic_ash_xml(data_root, data_url)
                    if volcano_data:
                        rows.append(volcano_data)
                except Exception as exc:
                    log.debug("Failed to parse volcanic ash XML from %s: %s", data_url, exc)
                    continue

            except Exception as exc:
                log.warning("Failed to process entry: %s", exc)
                continue

        df = pd.DataFrame(rows)
        log.info("Volcanic ash forecast rows fetched: %d", len(df))
        return df

    def _parse_volcanic_ash_xml(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VFVO53 volcanic ash forecast XML and extract forecast data."""
        def sn(tag):
            """Strip namespace from tag."""
            return tag.split('}')[-1] if '}' in tag else tag

        # Find report metadata in Head
        head = root.find('.//{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Head')
        if head is None:
            head = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Head')

        if head is None:
            return None

        # Extract header info
        event_id = None
        report_datetime = None
        volcano_name = None

        for elem in head.iter():
            tag = sn(elem.tag)
            if tag == 'EventID' and elem.text:
                event_id = elem.text
            elif tag == 'ReportDateTime' and elem.text:
                report_datetime = elem.text

        if not event_id:
            return None  # Not a valid volcanic ash event

        # Find body
        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/volcanology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        # Extract volcano name from VolcanoInfo
        for elem in body.iter():
            tag = sn(elem.tag)
            if tag == 'Area':
                for child in elem:
                    if sn(child.tag) == 'Name' and child.text:
                        volcano_name = child.text
                        break
                if volcano_name:
                    break

        # Extract ash forecast windows
        ash_data = {
            'event_id': event_id,
            'report_datetime': report_datetime,
            'volcano_name': volcano_name or '',
        }

        # Find all AshInfo entries (should be 6 time windows)
        ash_infos = []
        for elem in body.iter():
            tag = sn(elem.tag)
            if tag == 'AshInfo':
                ash_infos.append(elem)

        # Process up to 6 forecast windows
        for window_idx, ash_info in enumerate(ash_infos[:6], 1):
            start_time = None
            end_time = None
            affected_areas = []

            for child in ash_info:
                child_tag = sn(child.tag)
                if child_tag == 'StartTime' and child.text:
                    start_time = child.text
                elif child_tag == 'EndTime' and child.text:
                    end_time = child.text
                elif child_tag == 'Item':
                    # Extract affected areas
                    for item_child in child:
                        if sn(item_child.tag) == 'Areas':
                            for area in item_child:
                                if sn(area.tag) == 'Area':
                                    for area_child in area:
                                        if sn(area_child.tag) == 'Name' and area_child.text:
                                            affected_areas.append(area_child.text)

            # Store window data
            ash_data[f'window_{window_idx}_start'] = start_time
            ash_data[f'window_{window_idx}_end'] = end_time
            ash_data[f'window_{window_idx}_areas'] = ', '.join(affected_areas) if affected_areas else ''

        return ash_data


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
