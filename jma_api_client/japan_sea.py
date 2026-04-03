"""
Sea warning and forecast data fetching from JMA APIs.

Includes two official resources:
1. 地方海上警報 (Regional Sea Alert) - Data Type Code: VPCU51
2. 地方海上予報 (Regional Sea Forecast) - Data Type Code: VPCY51

Source Feed: other_l.xml
"""

import os
import xml.etree.ElementTree as ET

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed

import config
from logger import get_logger
from .utils import get
from .translate import translate_ja_to_en

log = get_logger(__name__)


@retry(stop=stop_after_attempt(config.RETRY_ATTEMPTS),
       wait=wait_fixed(config.RETRY_WAIT_SECONDS))
def fetch_sea_warnings() -> pd.DataFrame:
    """Fetch sea warnings from JMA XML feed (other_l.xml).

    Parses VPCU51 entries (regional sea warnings).

    Fields:
    - event_id, report_datetime, warning_type, region_name, region_code
    """
    return _fetch_sea_data('VPCU51', 'sea_warnings')


@retry(stop=stop_after_attempt(config.RETRY_ATTEMPTS),
       wait=wait_fixed(config.RETRY_WAIT_SECONDS))
def fetch_sea_forecasts() -> pd.DataFrame:
    """Fetch sea forecasts from JMA XML feed (other_l.xml).

    Parses VPCY51 entries (regional sea forecasts).

    Fields:
    - event_id, report_datetime, forecast_type, region_name, region_code
    """
    return _fetch_sea_data('VPCY51', 'sea_forecasts')


def _fetch_sea_data(data_type: str, feed_type: str) -> pd.DataFrame:
    """Generic method to fetch sea warning/forecast data from other_l.xml."""
    # Read locally saved other_l.xml feed
    feed_path = os.path.join(config.RAW_DATA_DIR, "other_l.xml")

    if not os.path.exists(feed_path):
        log.warning("other_l.xml not found in %s, skipping %s", config.RAW_DATA_DIR, feed_type)
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

    rows = []
    entry_count = 0

    # Process each feed entry
    for entry in entries:
        try:
            link = entry.find('atom:link', ns)
            if link is None:
                continue

            data_url = link.get('href')
            if not data_url or data_type not in data_url:
                continue

            entry_count += 1
            if entry_count > 250:  # Limit entries per type
                break

            # Fetch and parse the detailed XML data file
            try:
                data_resp = get(data_url)
                data_root = ET.fromstring(data_resp.content)
                sea_data = _parse_sea_xml(data_root, data_type)
                if sea_data:
                    rows.append(sea_data)
            except Exception as exc:
                log.debug("Failed to parse sea data XML from %s: %s", data_url, exc)
                continue

        except Exception as exc:
            log.warning("Failed to process entry: %s", exc)
            continue

    df = pd.DataFrame(rows)
    log.info("%s rows fetched: %d", feed_type, len(df))
    return df


def _parse_sea_xml(root: ET.Element, data_type: str) -> dict | None:
    """Parse JMA VPCU51/VPCY51 sea warning/forecast XML."""
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
    report_datetime = None

    for elem in head.iter():
        tag = sn(elem.tag)
        if tag == 'ReportDateTime' and elem.text:
            report_datetime = elem.text

    if not report_datetime:
        return None

    # Find body
    body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/meteorology1/}Body')
    if body is None:
        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

    if body is None:
        return None

    sea_data = {
        'event_id': f"{report_datetime}_{data_type}",
        'report_datetime': report_datetime,
    }

    # Extract warning/forecast type and affected region
    warning_type_key = 'warning_type' if data_type == 'VPCU51' else 'forecast_type'
    warning_type_en_key = warning_type_key + '_en'

    for elem in body.iter():
        tag = sn(elem.tag)

        if tag == 'Warning' or tag == 'Forecast':
            # Extract warning/forecast info from nested Item
            for item_elem in elem:
                if sn(item_elem.tag) == 'Item':
                    for item_child in item_elem:
                        item_child_tag = sn(item_child.tag)
                        if item_child_tag == 'Kind':
                            for kind_child in item_child:
                                if sn(kind_child.tag) == 'Name' and kind_child.text:
                                    sea_data[warning_type_key] = kind_child.text
                                    sea_data[warning_type_en_key] = translate_ja_to_en(kind_child.text)
                        elif item_child_tag == 'Area':
                            for area_child in item_child:
                                area_child_tag = sn(area_child.tag)
                                if area_child_tag == 'Name' and area_child.text:
                                    sea_data['region_name'] = area_child.text
                                    sea_data['region_name_en'] = translate_ja_to_en(area_child.text)
                                elif area_child_tag == 'Code' and area_child.text:
                                    sea_data['region_code'] = area_child.text

    return sea_data if len(sea_data) > 2 else None
