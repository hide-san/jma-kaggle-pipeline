"""
Earthquake and seismic data from JMA.

Official Resources:
1. 震源・震度に関する情報 (Earthquake & Seismic Intensity Information) - VXSE53
2. 震度速報 (Seismic Intensity Report) - VXSE51
3. 津波警報・注意報・予報 (Tsunami Warning/Advisory/Forecast) - VTSE41
4. 津波情報 (Tsunami Information) - VTSE51

Source Feed: eqvol_l.xml

Also fetches and caches all 4 JMA Data Feeds for use by other modules.
"""

import json
import os
import re
import xml.etree.ElementTree as ET

import pandas as pd

import config
from logger import get_logger
from .base import JMADatasetBase, register_dataset
from .utils import get, is_numeric, parse_latlon, save_raw
from .translate import translate_ja_to_en

log = get_logger(__name__)

__all__ = [
    "EarthquakeIntensityInfo",
    "SeismicIntensityReport",
    "TsunamiWarning",
    "EarthquakeEarlyWarning",
    "TsunamiInfo",
    "EarthquakeActivityInfo",
    "SeismicObservationInfo",
    "fetch_earthquake_data",
]


def fetch_earthquake_data() -> pd.DataFrame:
    """
    Fetch and cache all 4 JMA Data Feeds + JSON endpoint.

    This is a shared resource for all other modules that depend on cached feeds.
    It also provides simple earthquake data from the JSON endpoint as fallback.

    Returns:
        DataFrame with basic earthquake info from JSON endpoint (origin_time, magnitude, etc.)
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


@register_dataset
class EarthquakeIntensityInfo(JMADatasetBase):
    """
    Detailed earthquake and seismic intensity information from JMA VXSE53.

    Includes hypocenter coordinates, magnitude type, and per-prefecture intensity.
    """

    NAME = "japan-earthquake-and-seismic-information"
    CSV_FILENAME = "japan_earthquake_and_seismic_information.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VXSE53",)
    MERGE_KEYS = ["event_id"]
    DESCRIPTION = "Comprehensive earthquake and seismic intensity data from Japan Meteorological Agency (VXSE53). Contains hypocenter coordinates (latitude/longitude/depth), magnitude values, maximum seismic intensity, and detailed per-prefecture intensity observations. Updated with each significant earthquake event."
    SUBTITLE = "Detailed earthquake hypocenter, magnitude (Mjma), and per-prefecture seismic intensity observations"
    KEYWORDS = ["jma", "japan", "earthquake", "seismic", "intensity", "hypocenter", "magnitude", "hazard"]
    MAX_ENTRIES = 50

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VXSE53 earthquake XML and extract rich earthquake data."""
        # Extract header info
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        # Find body (try seismology1 namespace first)
        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/seismology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        eq_data = head_data.copy()

        # Extract Earthquake data from Body
        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'OriginTime' and elem.text:
                eq_data['origin_time'] = elem.text

            elif tag == 'Hypocenter':
                # Parse hypocenter (Area/Coordinate structure)
                for area_elem in elem:
                    if self.sn(area_elem.tag) == 'Area':
                        for area_child in area_elem:
                            child_tag = self.sn(area_child.tag)
                            if child_tag == 'Name' and area_child.text:
                                eq_data['hypocenter_area'] = area_child.text
                                eq_data['hypocenter_area_en'] = self.translate(area_child.text)
                            elif child_tag == 'Coordinate' and area_child.text:
                                # Parse "+lat+lon-depth/" format
                                coord_str = area_child.text.strip().replace('/', '')
                                try:
                                    coords = re.findall(r'[+-]?\d+\.?\d*', coord_str)
                                    if len(coords) >= 2:
                                        eq_data['hypocenter_latitude'] = float(coords[0])
                                        eq_data['hypocenter_longitude'] = float(coords[1])
                                        if len(coords) >= 3:
                                            # Depth is in meters, convert to km
                                            depth_m = abs(float(coords[2]))
                                            eq_data['hypocenter_depth_km'] = depth_m / 1000
                                except (ValueError, IndexError):
                                    pass

            elif tag == 'Magnitude':
                if elem.text:
                    try:
                        eq_data['magnitude'] = float(elem.text)
                    except ValueError:
                        pass
                mag_type = elem.get('type')
                if mag_type:
                    eq_data['magnitude_type'] = mag_type

            elif tag == 'MaxInt' and elem.text:
                eq_data['max_intensity'] = elem.text

        # Extract per-prefecture intensity data
        prefectures = {}
        prefectures_en = {}
        for elem in body.iter():
            tag = self.sn(elem.tag)
            if tag == 'Pref':
                pref_name = None
                pref_intensity = None
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Name' and child.text:
                        pref_name = child.text
                    elif child_tag == 'MaxInt' and child.text:
                        pref_intensity = child.text
                if pref_name and pref_intensity:
                    prefectures[pref_name] = pref_intensity
                    pref_name_en = self.translate(pref_name)
                    prefectures_en[pref_name_en] = pref_intensity

        if prefectures:
            eq_data['prefectures_intensity_json'] = json.dumps(prefectures, ensure_ascii=False)
            eq_data['prefectures_intensity_en_json'] = json.dumps(prefectures_en, ensure_ascii=False)

        return eq_data if len(eq_data) > 1 else None


@register_dataset
class SeismicIntensityReport(JMADatasetBase):
    """Seismic intensity rapid reports from JMA VXSE51."""

    NAME = "japan-seismic-intensity-report"
    CSV_FILENAME = "japan_seismic_intensity_report.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VXSE51",)
    MERGE_KEYS = ["event_id"]
    DESCRIPTION = "Rapid seismic intensity reports (VXSE51) issued within minutes of earthquake detection. Provides preliminary maximum seismic intensity across Japan with per-prefecture observations. Essential for rapid hazard assessment and early warning."
    SUBTITLE = "Fast preliminary seismic intensity reports with per-prefecture maximum intensity values"
    KEYWORDS = ["jma", "japan", "earthquake", "seismic", "intensity", "report", "rapid", "early-warning"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VXSE51 seismic intensity report XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/seismology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        report_data = head_data.copy()

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'OriginTime' and elem.text:
                report_data['origin_time'] = elem.text

            elif tag == 'Magnitude':
                if elem.text:
                    try:
                        report_data['magnitude'] = float(elem.text)
                    except ValueError:
                        pass

            elif tag == 'MaxInt' and elem.text:
                report_data['max_intensity'] = elem.text

            elif tag == 'Pref':
                # Parse prefecture intensities
                pref_name = None
                pref_intensity = None
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Name' and child.text:
                        pref_name = child.text
                    elif child_tag == 'MaxInt' and child.text:
                        pref_intensity = child.text

        return report_data if len(report_data) > 2 else None


@register_dataset
class TsunamiWarning(JMADatasetBase):
    """Tsunami warning and advisory information from JMA VTSE41."""

    NAME = "japan-tsunami-warning"
    CSV_FILENAME = "japan_tsunami_warning.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VTSE41",)
    MERGE_KEYS = ["event_id"]
    DESCRIPTION = "Tsunami warnings, advisories, and forecasts (VTSE41) for Japanese coasts. Provides estimated wave heights by coastal region, arrival times, and threat levels. Critical for coastal hazard management and public safety."
    SUBTITLE = "Coastal tsunami threat alerts with estimated wave heights and arrival times by region"
    KEYWORDS = ["jma", "japan", "tsunami", "warning", "advisory", "coastal", "wave-height", "hazard"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VTSE41 tsunami warning XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/tsunami1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        tsunami_data = head_data.copy()

        # Extract tsunami warnings by area
        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'OriginTime' and elem.text:
                tsunami_data['origin_time'] = elem.text

            elif tag == 'Hypocenter':
                for area_elem in elem:
                    if self.sn(area_elem.tag) == 'Area':
                        for area_child in area_elem:
                            child_tag = self.sn(area_child.tag)
                            if child_tag == 'Name' and area_child.text:
                                tsunami_data['hypocenter_area'] = area_child.text
                                tsunami_data['hypocenter_area_en'] = self.translate(area_child.text)
                            elif child_tag == 'Coordinate' and area_child.text:
                                coord_str = area_child.text.strip().replace('/', '')
                                try:
                                    coords = re.findall(r'[+-]?\d+\.?\d*', coord_str)
                                    if len(coords) >= 2:
                                        tsunami_data['hypocenter_latitude'] = float(coords[0])
                                        tsunami_data['hypocenter_longitude'] = float(coords[1])
                                except (ValueError, IndexError):
                                    pass

            elif tag == 'Magnitude' and elem.text:
                try:
                    tsunami_data['magnitude'] = float(elem.text)
                except ValueError:
                    pass

            elif tag == 'Tsunami':
                for item in elem:
                    if self.sn(item.tag) == 'Item':
                        for item_child in item:
                            item_tag = self.sn(item_child.tag)
                            if item_tag == 'Kind':
                                for kind_child in item_child:
                                    if self.sn(kind_child.tag) == 'Name' and kind_child.text:
                                        tsunami_data['warning_type'] = kind_child.text
                                        tsunami_data['warning_type_en'] = self.translate(kind_child.text)
                            elif item_tag == 'Areas':
                                areas = []
                                for area in item_child:
                                    if self.sn(area.tag) == 'Area':
                                        for area_child in area:
                                            if self.sn(area_child.tag) == 'Name' and area_child.text:
                                                areas.append(area_child.text)
                                if areas:
                                    tsunami_data['affected_areas_json'] = json.dumps(areas, ensure_ascii=False)
                                    areas_en = [self.translate(a) for a in areas]
                                    tsunami_data['affected_areas_en_json'] = json.dumps(areas_en, ensure_ascii=False)

        return tsunami_data if len(tsunami_data) > 2 else None


@register_dataset
class EarthquakeEarlyWarning(JMADatasetBase):
    """Earthquake early warning alerts from JMA VXSE43/VXSE44."""

    NAME = "japan-earthquake-early-warning"
    CSV_FILENAME = "japan_earthquake_early_warning.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VXSE43", "VXSE44")
    MERGE_KEYS = ["event_id"]
    DESCRIPTION = "Real-time Earthquake Early Warning (EEW) alerts (VXSE43/44) issued seconds after earthquake detection. Predicts strong motion intensities by region to enable automated protective actions. Updated continuously as more data arrives."
    SUBTITLE = "Real-time EEW ground motion predictions with automated broadcast alerts for hazard mitigation"
    KEYWORDS = ["jma", "japan", "earthquake", "early-warning", "eew", "strong-motion", "real-time"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VXSE43/VXSE44 earthquake early warning XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/seismology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        warning_data = head_data.copy()

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'OriginTime' and elem.text:
                warning_data['origin_time'] = elem.text

            elif tag == 'Magnitude' and elem.text:
                try:
                    warning_data['magnitude'] = float(elem.text)
                except ValueError:
                    pass

            elif tag == 'MaxInt' and elem.text:
                warning_data['max_intensity'] = elem.text

            elif tag == 'Hypocenter':
                for area_elem in elem:
                    if self.sn(area_elem.tag) == 'Area':
                        for area_child in area_elem:
                            child_tag = self.sn(area_child.tag)
                            if child_tag == 'Name' and area_child.text:
                                warning_data['epicenter'] = area_child.text
                                warning_data['epicenter_en'] = self.translate(area_child.text)

        return warning_data if len(warning_data) > 2 else None


@register_dataset
class TsunamiInfo(JMADatasetBase):
    """Tsunami observation information from JMA VTSE51."""

    NAME = "japan-tsunami-information"
    CSV_FILENAME = "japan_tsunami_information.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VTSE51",)
    MERGE_KEYS = ["event_id"]
    DESCRIPTION = "Tsunami observation data (VTSE51) from coastal seismic stations and tide gauges across Japan. Records actual wave heights, arrival times, and propagation patterns for forecast validation and hazard assessment."
    SUBTITLE = "Observed tsunami wave heights and arrival times from coastal monitoring stations"
    KEYWORDS = ["jma", "japan", "tsunami", "observation", "wave-height", "coastal", "tide-gauge", "hazard"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VTSE51 tsunami information XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/tsunami1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        info_data = head_data.copy()
        observations = []

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'OriginTime' and elem.text:
                info_data['origin_time'] = elem.text

            elif tag == 'Observation':
                obs_dict = {}
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'DateTime' and child.text:
                        obs_dict['observation_time'] = child.text
                    elif child_tag == 'Station':
                        for st_child in child:
                            st_tag = self.sn(st_child.tag)
                            if st_tag == 'Name' and st_child.text:
                                obs_dict['station_name'] = st_child.text
                    elif child_tag == 'Height' and child.text:
                        obs_dict['wave_height'] = child.text

                if obs_dict:
                    observations.append(obs_dict)

        if observations:
            info_data['observations_json'] = json.dumps(observations, ensure_ascii=False)

        return info_data if len(info_data) > 2 else None


@register_dataset
class EarthquakeActivityInfo(JMADatasetBase):
    """Earthquake activity status information from JMA VXSE56."""

    NAME = "japan-earthquake-activity-information"
    CSV_FILENAME = "japan_earthquake_activity_information.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VXSE56",)
    MERGE_KEYS = ["event_id"]
    DESCRIPTION = "Earthquake activity status updates (VXSE56) summarizing recent seismic trends, aftershock sequences, and foreshock patterns. Includes frequency distribution and intensity statistics for ongoing seismic swarms."
    SUBTITLE = "Seismic activity summaries with aftershock sequence stats and ongoing trend reports"
    KEYWORDS = ["jma", "japan", "earthquake", "activity", "aftershock", "foreshock", "seismic-swarm", "trend"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VXSE56 earthquake activity information XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/seismology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        activity_data = head_data.copy()

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'DateTime' and elem.text:
                activity_data['report_time'] = elem.text

            elif tag == 'Text' and elem.text:
                activity_data['activity_description'] = elem.text.strip()
                activity_data['activity_description_en'] = self.translate(elem.text.strip())

        return activity_data if len(activity_data) > 2 else None


@register_dataset
class SeismicObservationInfo(JMADatasetBase):
    """Seismic observation status information from JMA VXSE60-62."""

    NAME = "japan-seismic-observation-information"
    CSV_FILENAME = "japan_seismic_observation_information.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VXSE60", "VXSE61", "VXSE62")
    MERGE_KEYS = ["event_id"]
    DESCRIPTION = "Seismic observation status (VXSE60-62) from nationwide network monitoring earthquake counts, long-period oscillation data, and instrument health. Provides operational summaries of network performance and anomalies."
    SUBTITLE = "Seismic network status with earthquake counts, long-period oscillations, and sensor health"
    KEYWORDS = ["jma", "japan", "seismic", "observation", "network", "long-period", "monitoring", "status"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VXSE60-62 seismic observation information XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/seismology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        obs_data = head_data.copy()

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'DateTime' and elem.text:
                obs_data['observation_time'] = elem.text

            elif tag == 'Text' and elem.text:
                obs_data['observation_detail'] = elem.text.strip()
                obs_data['observation_detail_en'] = self.translate(elem.text.strip())

        return obs_data if len(obs_data) > 2 else None


# For backwards compatibility
def fetch_earthquakes_enhanced() -> pd.DataFrame:
    """Legacy function wrapper."""
    return EarthquakeIntensityInfo().fetch()
