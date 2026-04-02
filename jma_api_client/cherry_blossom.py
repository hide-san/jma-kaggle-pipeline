"""Cherry blossom data fetching from JMA APIs."""

import os
import xml.etree.ElementTree as ET
from io import StringIO

import pandas as pd
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_fixed
import requests

import config
from logger import get_logger
from .utils import save_raw
from .translate import translate_ja_to_en

log = get_logger(__name__)


@retry(stop=stop_after_attempt(config.RETRY_ATTEMPTS),
       wait=wait_fixed(config.RETRY_WAIT_SECONDS))
def fetch_cherry_blossom_observations() -> pd.DataFrame:
    """Fetch cherry blossom observations from JMA XML feed (other_l.xml).

    Parses VGSK55 entries (biological observations) with phenophase and location data.

    Fields:
    - event_id, observation_date, report_datetime, phenophase, phenophase_code
    - station_name, station_location
    """
    # Read locally saved other_l.xml feed
    feed_path = os.path.join(config.RAW_DATA_DIR, "other_l.xml")

    if not os.path.exists(feed_path):
        log.warning("other_l.xml not found in %s, skipping cherry blossom observations", config.RAW_DATA_DIR)
        return pd.DataFrame()

    try:
        with open(feed_path, 'rb') as f:
            feed_content = f.read()
    except Exception as exc:
        log.error("Could not read other_l.xml: %s", exc)
        return pd.DataFrame()

    # Parse Atom feed
    try:
        root = ET.fromstring(feed_content)
    except ET.ParseError as exc:
        log.error("Could not parse other_l.xml: %s", exc)
        return pd.DataFrame()

    ns = {'atom': 'http://www.w3.org/2005/Atom'}
    entries = root.findall('.//atom:entry', ns)
    log.info("Found %d entries in other_l.xml", len(entries))

    rows = []
    entry_count = 0

    # Process each feed entry
    for entry in entries:
        try:
            link = entry.find('atom:link', ns)
            if link is None:
                continue

            data_url = link.get('href')
            if not data_url or 'VGSK55' not in data_url:  # Only process cherry blossom entries
                continue

            entry_count += 1
            if entry_count > 100:  # Limit to recent observations
                break

            # Fetch and parse the detailed XML data file
            try:
                resp = requests.get(data_url, timeout=30)
                resp.raise_for_status()
                data_root = ET.fromstring(resp.content)
                cherry_data = _parse_cherry_blossom_xml(data_root, data_url)
                if cherry_data:
                    rows.append(cherry_data)
            except Exception as exc:
                log.debug("Failed to parse cherry blossom XML from %s: %s", data_url, exc)
                continue

        except Exception as exc:
            log.warning("Failed to process entry: %s", exc)
            continue

    df = pd.DataFrame(rows)
    log.info("Cherry blossom observation rows fetched: %d", len(df))
    return df


def _parse_cherry_blossom_xml(root: ET.Element, data_url: str) -> dict | None:
    """Parse JMA VGSK55 cherry blossom observation XML and extract phenophase data."""
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

    for elem in head.iter():
        tag = sn(elem.tag)
        if tag == 'EventID' and elem.text:
            event_id = elem.text
        elif tag == 'ReportDateTime' and elem.text:
            report_datetime = elem.text

    if not event_id:
        return None

    # Find body
    body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/meteorology1/}Body')
    if body is None:
        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

    if body is None:
        return None

    cherry_data = {
        'event_id': event_id,
        'report_datetime': report_datetime,
    }

    # Find MeteorologicalInfo (contains observation data)
    for elem in body.iter():
        tag = sn(elem.tag)

        if tag == 'DateTime':
            # Extract observation date
            if elem.text:
                cherry_data['observation_date'] = elem.text

        elif tag == 'Kind':
            # Extract phenophase information
            for child in elem:
                child_tag = sn(child.tag)
                if child_tag == 'Name' and child.text:
                    cherry_data['phenophase'] = child.text
                    cherry_data['phenophase_en'] = translate_ja_to_en(child.text)
                elif child_tag == 'Code' and child.text:
                    cherry_data['phenophase_code'] = child.text

        elif tag == 'Station':
            # Extract station/location information
            for child in elem:
                child_tag = sn(child.tag)
                if child_tag == 'Name' and child.text:
                    cherry_data['station_name'] = child.text
                    cherry_data['station_name_en'] = translate_ja_to_en(child.text)
                elif child_tag == 'Location' and child.text:
                    cherry_data['station_location'] = child.text
                    cherry_data['station_location_en'] = translate_ja_to_en(child.text)

    return cherry_data if len(cherry_data) > 2 else None
