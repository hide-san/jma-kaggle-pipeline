"""Earthquake data fetching from JMA APIs."""

import json
import os
import re
import xml.etree.ElementTree as ET

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed

import config
from logger import get_logger
from .utils import get, is_numeric, parse_latlon, save_raw
from .translate import translate_ja_to_en

log = get_logger(__name__)


@retry(stop=stop_after_attempt(config.RETRY_ATTEMPTS),
       wait=wait_fixed(config.RETRY_WAIT_SECONDS))
def fetch_earthquake_data() -> pd.DataFrame:
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
            feed_resp = get(feed_url)
            save_raw(feed_name, feed_resp.content)
            log.info("Saved %s (%d bytes)", feed_name, len(feed_resp.content))
        except Exception as exc:
            log.warning("Could not fetch %s: %s", feed_name, exc)

    # Fetch earthquake list from simple JSON endpoint
    url = "https://www.jma.go.jp/bosai/quake/data/list.json"
    log.info("Fetching earthquake list: %s", url)
    resp = get(url)
    items = resp.json()  # list of dicts
    # Save raw response
    save_raw("earthquakes.json", resp.content)

    rows = []
    for item in items:
        # Derive a stable event_id from the hypocenter report key
        event_id = item.get("json", "").replace("/", "_").removesuffix(".json")
        at = item.get("at", "")          # origin time
        maxi = item.get("maxi", "")      # max seismic intensity
        mag_str = item.get("mag", "")
        mag = float(mag_str) if is_numeric(mag_str) else None
        anm = item.get("anm", "")        # epicentre name
        # lat/lon embedded in item for recent quakes
        cod = item.get("cod", "")        # "lat lon" string
        lat, lon = parse_latlon(cod)

        row = {
            "event_id": event_id,
            "origin_time": at,
            "epicentre": anm,
            "epicentre_en": translate_ja_to_en(anm) if anm else "",
            "latitude": lat,
            "longitude": lon,
            "magnitude": mag,
            "max_intensity": maxi,
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    log.info("Earthquake rows fetched: %d", len(df))
    return df


@retry(stop=stop_after_attempt(config.RETRY_ATTEMPTS),
       wait=wait_fixed(config.RETRY_WAIT_SECONDS))
def fetch_earthquakes_enhanced() -> pd.DataFrame:
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
                data_resp = get(data_url)
                data_root = ET.fromstring(data_resp.content)
                earthquake_data = _parse_earthquake_xml(data_root, data_url)
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


def _parse_earthquake_xml(root: ET.Element, data_url: str) -> dict | None:
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
                            eq_data['hypocenter_area_en'] = translate_ja_to_en(area_child.text)
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
    prefectures_en = {}
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
                pref_name_en = translate_ja_to_en(pref_name)
                prefectures_en[pref_name_en] = pref_intensity

    if prefectures:
        eq_data['prefectures_intensity_json'] = json.dumps(prefectures, ensure_ascii=False)
        eq_data['prefectures_intensity_en_json'] = json.dumps(prefectures_en, ensure_ascii=False)

    return eq_data if len(eq_data) > 1 else None  # Return only if has data beyond event_id
