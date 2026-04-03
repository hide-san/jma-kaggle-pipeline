"""Volcano data fetching from JMA APIs (status and ash forecasts)."""

import os
import xml.etree.ElementTree as ET

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed

import config
from logger import get_logger
from .utils import get, save_raw
from .translate import translate_ja_to_en

log = get_logger(__name__)


@retry(stop=stop_after_attempt(config.RETRY_ATTEMPTS),
       wait=wait_fixed(config.RETRY_WAIT_SECONDS))
def fetch_volcanic_ash_forecasts() -> pd.DataFrame:
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
                data_resp = get(data_url)
                data_root = ET.fromstring(data_resp.content)
                volcano_data = _parse_volcanic_ash_xml(data_root, data_url)
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


def _parse_volcanic_ash_xml(root: ET.Element, data_url: str) -> dict | None:
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
        'volcano_name_en': translate_ja_to_en(volcano_name) if volcano_name else '',
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

        # Store window data (keep original and add English translation)
        ash_data[f'window_{window_idx}_start'] = start_time
        ash_data[f'window_{window_idx}_end'] = end_time
        ash_data[f'window_{window_idx}_areas'] = ', '.join(affected_areas) if affected_areas else ''

        # Add English translations
        translated_areas = [translate_ja_to_en(area) for area in affected_areas]
        ash_data[f'window_{window_idx}_areas_en'] = ', '.join(translated_areas) if translated_areas else ''

    return ash_data


@retry(stop=stop_after_attempt(config.RETRY_ATTEMPTS),
       wait=wait_fixed(config.RETRY_WAIT_SECONDS))
def fetch_volcano_status() -> pd.DataFrame:
    """Fetch volcano status reports from JMA XML feed (eqvol_l.xml).

    Parses VFVO51 entries (volcano status) with alert levels and activity data.

    Fields:
    - event_id, report_datetime, volcano_name
    - alert_level, alert_level_code, alert_condition
    - activity_summary, prevention_summary
    """
    # Read locally saved eqvol_l.xml feed
    feed_path = os.path.join(config.RAW_DATA_DIR, "eqvol_l.xml")

    if not os.path.exists(feed_path):
        log.warning("eqvol_l.xml not found in %s, skipping volcano status", config.RAW_DATA_DIR)
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
            if not data_url or 'VFVO51' not in data_url:  # Only process volcano status entries
                continue

            entry_count += 1

            # Fetch and parse the detailed XML data file
            try:
                data_resp = get(data_url)
                data_root = ET.fromstring(data_resp.content)
                volcano_data = _parse_volcano_status_xml(data_root, data_url)
                if volcano_data:
                    rows.append(volcano_data)
            except Exception as exc:
                log.debug("Failed to parse volcano status XML from %s: %s", data_url, exc)
                continue

        except Exception as exc:
            log.warning("Failed to process entry: %s", exc)
            continue

    df = pd.DataFrame(rows)
    log.info("Volcano status rows fetched: %d", len(df))
    return df


def _parse_volcano_status_xml(root: ET.Element, data_url: str) -> dict | None:
    """Parse JMA VFVO51 volcano status XML and extract volcano alert data."""
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
    body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/volcanology1/}Body')
    if body is None:
        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

    if body is None:
        return None

    volcano_data = {
        'event_id': event_id,
        'report_datetime': report_datetime,
    }

    # Extract volcano info and alert level
    for elem in body.iter():
        tag = sn(elem.tag)

        # Get volcano name and alert level from VolcanoInfo
        if tag == 'VolcanoInfo':
            for child in elem:
                if sn(child.tag) == 'Item':
                    # Extract alert level info
                    for item_child in child:
                        if sn(item_child.tag) == 'Kind':
                            for kind_child in item_child:
                                kind_tag = sn(kind_child.tag)
                                if kind_tag == 'Name' and kind_child.text:
                                    volcano_data['alert_level'] = kind_child.text
                                    volcano_data['alert_level_en'] = translate_ja_to_en(kind_child.text)
                                elif kind_tag == 'Code' and kind_child.text:
                                    volcano_data['alert_level_code'] = kind_child.text
                                elif kind_tag == 'Condition' and kind_child.text:
                                    volcano_data['alert_condition'] = kind_child.text
                                    volcano_data['alert_condition_en'] = translate_ja_to_en(kind_child.text)

                    # Extract volcano name from Areas
                    for item_child in child:
                        if sn(item_child.tag) == 'Areas':
                            for area in item_child:
                                if sn(area.tag) == 'Area':
                                    for area_child in area:
                                        if sn(area_child.tag) == 'Name' and area_child.text:
                                            volcano_data['volcano_name'] = area_child.text
                                            volcano_data['volcano_name_en'] = translate_ja_to_en(area_child.text)
                                            break

        # Get activity and prevention summaries from VolcanoInfoContent
        elif tag == 'VolcanoInfoContent':
            for content_child in elem:
                content_tag = sn(content_child.tag)
                if content_tag == 'VolcanoActivity' and content_child.text:
                    activity_text = content_child.text.strip()
                    volcano_data['activity_summary'] = activity_text
                    volcano_data['activity_summary_en'] = translate_ja_to_en(activity_text)
                elif content_tag == 'VolcanoPrevention' and content_child.text:
                    prevention_text = content_child.text.strip()
                    volcano_data['prevention_summary'] = prevention_text
                    volcano_data['prevention_summary_en'] = translate_ja_to_en(prevention_text)

    return volcano_data if len(volcano_data) > 2 else None
