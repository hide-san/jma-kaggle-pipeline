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
from .utils import get


log = get_logger(__name__)

__all__ = [
    "EarthquakeIntensityInfo",
    "SeismicIntensityReport",
    "TsunamiWarning",
    "EarthquakeEarlyWarning",
    "TsunamiInfo",
    "EarthquakeActivityInfo",
    "SeismicObservationInfo",
]


@register_dataset
class EarthquakeIntensityInfo(JMADatasetBase):
    """
    Detailed earthquake and seismic intensity information from JMA VXSE53.

    Includes hypocenter coordinates, magnitude type, and per-prefecture intensity.
    """

    NAME = "jma-hypocenter-and-seismic-intensity"
    CSV_FILENAME = "jma_hypocenter_and_seismic_intensity.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VXSE53",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Hypocenter and Seismic Intensity"
    DESCRIPTION = (
        "震源・震度に関する情報 — Earthquake hypocenter and seismic intensity reports issued by the "
        "Japan Meteorological Agency (JMA) for every significant seismic event across Japan. "
        "Each record corresponds to one earthquake and captures the full official report at the time of publication.\n\n"
        "**Columns include:** event_id, origin_time, title (ja/en), info_type (ja/en), "
        "hypocenter_area (ja/en), hypocenter_latitude, hypocenter_longitude, hypocenter_depth_km, "
        "magnitude, magnitude_type (Mjma/Mj/Mw), max_intensity (震度 scale), "
        "prefectures_intensity_json (ja/en — per-prefecture max intensity as JSON)\n\n"
        "**Feed:** eqvol_l.xml | **Type code:** VXSE53\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 50\n"
        "**Use cases:** Seismological research, hazard mapping, ML earthquake pattern analysis, "
        "disaster risk assessment"
    )
    SUBTITLE = "Earthquake hypocenter, magnitude (Mjma), and per-prefecture seismic intensity"
    KEYWORDS = ["japan", "earthquake", "natural-disaster", "public-safety", "east-asia"]
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

    NAME = "jma-seismic-intensity-flash-report"
    CSV_FILENAME = "jma_seismic_intensity_flash_report.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VXSE51",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Seismic Intensity Flash Report"
    DESCRIPTION = (
        "震度速報 — Rapid seismic intensity reports published by JMA within minutes of earthquake detection, "
        "before full hypocenter analysis is complete. Provides preliminary maximum seismic intensity (震度) "
        "across Japan with per-prefecture breakdown for immediate hazard assessment.\n\n"
        "**Columns include:** event_id, report_datetime, title (ja/en), info_type (ja/en), "
        "origin_time, magnitude, max_intensity, "
        "prefectures_intensity_json (ja/en — per-prefecture max intensity as JSON)\n\n"
        "**Feed:** eqvol_l.xml | **Type code:** VXSE51\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** Real-time hazard monitoring, early warning system research, "
        "rapid damage estimation, public safety alerts"
    )
    SUBTITLE = "Fast seismic intensity reports with per-prefecture maximum intensity values"
    KEYWORDS = ["japan", "earthquake", "natural-disaster", "public-safety", "emergency-response"]
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
                    prefectures_en[self.translate(pref_name)] = pref_intensity

        if prefectures:
            report_data['prefectures_intensity_json'] = json.dumps(prefectures, ensure_ascii=False)
            report_data['prefectures_intensity_en_json'] = json.dumps(prefectures_en, ensure_ascii=False)

        return report_data if len(report_data) > 2 else None


@register_dataset
class TsunamiWarning(JMADatasetBase):
    """Tsunami warning and advisory information from JMA VTSE41."""

    NAME = "jma-tsunami-warning-advisory-forecast"
    CSV_FILENAME = "jma_tsunami_warning_advisory_forecast.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VTSE41",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Tsunami Warning Advisory Forecast"
    DESCRIPTION = (
        "津波警報・注意報・予報 — Official tsunami warnings, advisories, and forecasts issued by JMA "
        "for Japanese coastal regions following significant seismic events. Includes estimated wave heights, "
        "affected coastal areas, and the triggering earthquake's hypocenter data.\n\n"
        "**Columns include:** event_id, report_datetime, title (ja/en), info_type (ja/en), "
        "origin_time, hypocenter_area (ja/en), hypocenter_latitude, hypocenter_longitude, magnitude, "
        "warning_type (ja/en), affected_areas_json (ja/en — list of affected coastal regions)\n\n"
        "**Feed:** eqvol_l.xml | **Type code:** VTSE41\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** Coastal hazard research, tsunami warning system analysis, "
        "evacuation planning, disaster risk management"
    )
    SUBTITLE = "Coastal tsunami alerts with estimated wave heights and arrival times by region"
    KEYWORDS = ["japan", "natural-disaster", "disaster", "public-safety", "ocean"]
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

    NAME = "jma-earthquake-early-warning"
    CSV_FILENAME = "jma_earthquake_early_warning.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VXSE43", "VXSE44")
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Earthquake Early Warning"
    DESCRIPTION = (
        "緊急地震速報 — Earthquake Early Warning (EEW) alerts broadcast by JMA seconds after P-wave "
        "detection, before strong shaking arrives. Predicts ground motion intensities by region to "
        "enable automated protective actions in trains, factories, and homes. "
        "VXSE43 covers general alerts; VXSE44 covers prediction updates.\n\n"
        "**Columns include:** event_id, report_datetime, title (ja/en), info_type (ja/en), "
        "origin_time, epicenter (ja/en), magnitude, max_intensity\n\n"
        "**Feed:** eqvol_l.xml | **Type codes:** VXSE43, VXSE44\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** EEW system performance analysis, lead-time research, "
        "automated protective action studies, seismic risk engineering"
    )
    SUBTITLE = "Real-time EEW predictions with automated broadcast alerts for hazard mitigation"
    KEYWORDS = ["japan", "earthquake", "natural-disaster", "public-safety", "alerts"]
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

    NAME = "jma-tsunami-information"
    CSV_FILENAME = "jma_tsunami_information.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VTSE51",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Tsunami Information"
    DESCRIPTION = (
        "津波情報 — Tsunami observation reports published by JMA from coastal seismic stations and "
        "tide gauges throughout Japan. Records confirmed wave heights, arrival times, and propagation "
        "characteristics used for forecast validation and post-event hazard assessment.\n\n"
        "**Columns include:** event_id, report_datetime, title (ja/en), info_type (ja/en), "
        "origin_time, observations_json (station_name, observation_time, wave_height per station)\n\n"
        "**Feed:** eqvol_l.xml | **Type code:** VTSE51\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** Tsunami propagation research, tide gauge network analysis, "
        "forecast model validation, coastal engineering"
    )
    SUBTITLE = "Observed tsunami wave heights and arrival times from coastal monitoring stations"
    KEYWORDS = ["japan", "natural-disaster", "ocean", "sea", "public-safety"]
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

    NAME = "jma-earthquake-activity-status"
    CSV_FILENAME = "jma_earthquake_activity_status.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VXSE56",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Earthquake Activity Status"
    DESCRIPTION = (
        "震源活動特性解説情報 — Earthquake activity status reports published by JMA summarising "
        "ongoing seismic trends, aftershock sequences, foreshock patterns, and swarm activity. "
        "Provides narrative analysis of seismic conditions following significant events.\n\n"
        "**Columns include:** event_id, report_datetime, title (ja/en), info_type (ja/en), "
        "report_time, activity_description (ja/en — full narrative text)\n\n"
        "**Feed:** eqvol_l.xml | **Type code:** VXSE56\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** Aftershock sequence analysis, seismic swarm monitoring, "
        "public communication research, NLP on Japanese disaster text"
    )
    SUBTITLE = "Seismic activity summaries with aftershock stats and ongoing trend reports"
    KEYWORDS = ["japan", "earthquake", "natural-disaster", "east-asia", "government"]
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

    NAME = "jma-earthquake-frequency-information"
    CSV_FILENAME = "jma_earthquake_frequency_information.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VXSE60", "VXSE61", "VXSE62")
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Earthquake Frequency Information"
    DESCRIPTION = (
        "地震観測情報 — Seismic observation status reports (VXSE60-62) from JMA's nationwide "
        "monitoring network. Covers weekly earthquake count summaries (VXSE60), long-period ground "
        "motion observations (VXSE61), and seismic instrument operational status (VXSE62).\n\n"
        "**Columns include:** event_id, report_datetime, title (ja/en), info_type (ja/en), "
        "observation_time, observation_detail (ja/en)\n\n"
        "**Feed:** eqvol_l.xml | **Type codes:** VXSE60, VXSE61, VXSE62\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** Seismic network monitoring, long-period oscillation research, "
        "infrastructure health tracking, earthquake catalog cross-referencing"
    )
    SUBTITLE = "Seismic network status with earthquake counts and long-period oscillations"
    KEYWORDS = ["japan", "earthquake", "natural-disaster", "east-asia", "remote-sensing"]
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
