"""
Typhoon and tropical cyclone data from JMA.

Official Resources:
1. 全般台風情報 (General Typhoon Information) - VPTI50
2. 全般台風情報（定型） (General Typhoon Information - Standardized) - VPTI51
3. 全般台風情報（詳細） (General Typhoon Information - Detailed) - VPTI52

Source Feed: regular_l.xml
"""

import xml.etree.ElementTree as ET

import pandas as pd

from .base import JMADatasetBase, register_dataset

__all__ = [
    "TyphoonInfoGeneral",
    "TyphoonInfoStandardized",
    "TyphoonInfoDetailed",
]


@register_dataset
class TyphoonInfoGeneral(JMADatasetBase):
    """General typhoon information from JMA VPTI50."""

    NAME = "jma-general-typhoon-information"
    CSV_FILENAME = "jma_general_typhoon_information.csv"
    FEED_NAME = "regular_l.xml"
    TYPE_CODES = ("VPTI50",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA General Typhoon Information"
    DESCRIPTION = (
        "全般台風情報 — General Typhoon Information. "
        "JMA typhoon bulletins with current position, central pressure, maximum wind speed, "
        "and forecast track for tropical cyclones affecting Japan.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, title_en, "
        "forecast_time, central_pressure_hpa, max_wind_speed_kmh, forecast_direction_en\n\n"
        "**Feed:** regular_l.xml | **Type code:** VPTI50\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** typhoon tracking, landfall prediction, disaster preparedness, "
        "climate research, storm intensity analysis"
    )
    SUBTITLE = "Typhoon position, intensity (pressure/wind), and forecast track information"
    KEYWORDS = ["japan", "weather", "disaster", "natural-disaster", "east-asia"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VPTI50 general typhoon information XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/meteorology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        typhoon_data = head_data.copy()

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'DateTime' and elem.text:
                typhoon_data['forecast_time'] = elem.text

            elif tag == 'TyphoonInfo':
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Item':
                        for item_child in child:
                            item_tag = self.sn(item_child.tag)
                            if item_tag == 'Pressure' and item_child.text:
                                typhoon_data['central_pressure_hpa'] = item_child.text
                            elif item_tag == 'Wind' and item_child.text:
                                typhoon_data['max_wind_speed_kmh'] = item_child.text
                            elif item_tag == 'Circle':
                                for circle_child in item_child:
                                    circle_tag = self.sn(circle_child.tag)
                                    if circle_tag == 'Direction' and circle_child.text:
                                        typhoon_data['forecast_direction'] = circle_child.text
                                        typhoon_data['forecast_direction_en'] = self.translate(circle_child.text)

        return typhoon_data if len(typhoon_data) > 2 else None


@register_dataset
class TyphoonInfoStandardized(JMADatasetBase):
    """Standardized typhoon information from JMA VPTI51."""

    NAME = "jma-general-typhoon-information-standardized"
    CSV_FILENAME = "jma_general_typhoon_information_standardized.csv"
    FEED_NAME = "regular_l.xml"
    TYPE_CODES = ("VPTI51",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA General Typhoon Information Standardized"
    DESCRIPTION = (
        "全般台風情報（定型） — Standardized Typhoon Information. "
        "JMA typhoon data in a fixed structured format for automated processing, "
        "providing pressure, wind speed, and observation times in a consistent schema.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, title_en, "
        "observation_time, central_pressure_hpa, max_wind_speed_kmh\n\n"
        "**Feed:** regular_l.xml | **Type code:** VPTI51\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** automated typhoon monitoring, time-series analysis, "
        "machine learning datasets, structured storm data"
    )
    SUBTITLE = "Standardized-format typhoon data with pressure and wind speed"
    KEYWORDS = ["japan", "weather", "disaster", "east-asia", "wind"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VPTI51 standardized typhoon information XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/meteorology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        typhoon_data = head_data.copy()

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'DateTime' and elem.text:
                typhoon_data['observation_time'] = elem.text

            elif tag == 'TyphoonInfo':
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Pressure' and child.text:
                        typhoon_data['central_pressure_hpa'] = child.text
                    elif child_tag == 'Wind' and child.text:
                        typhoon_data['max_wind_speed_kmh'] = child.text

        return typhoon_data if len(typhoon_data) > 2 else None


@register_dataset
class TyphoonInfoDetailed(JMADatasetBase):
    """Detailed typhoon information from JMA VPTI52."""

    NAME = "jma-general-typhoon-information-detailed"
    CSV_FILENAME = "jma_general_typhoon_information_detailed.csv"
    FEED_NAME = "regular_l.xml"
    TYPE_CODES = ("VPTI52",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA General Typhoon Information Detailed"
    DESCRIPTION = (
        "全般台風情報（詳細） — Detailed Typhoon Information. "
        "Comprehensive JMA typhoon data with extended forecast track positions, latitude/longitude, "
        "pressure, and wind speed for serious typhoon impacts on Japan.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, title_en, "
        "forecast_time, central_pressure_hpa, max_wind_speed_kmh, latitude, longitude\n\n"
        "**Feed:** regular_l.xml | **Type code:** VPTI52\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** typhoon track modeling, impact assessment, "
        "GIS visualization, wind field analysis"
    )
    SUBTITLE = "Detailed typhoon forecasts with extended track and impact predictions"
    KEYWORDS = ["japan", "weather", "disaster", "natural-disaster", "gis"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VPTI52 detailed typhoon information XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/meteorology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        typhoon_data = head_data.copy()

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'DateTime' and elem.text:
                typhoon_data['forecast_time'] = elem.text

            elif tag == 'TyphoonInfo':
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Pressure' and child.text:
                        typhoon_data['central_pressure_hpa'] = child.text
                    elif child_tag == 'Wind' and child.text:
                        typhoon_data['max_wind_speed_kmh'] = child.text
                    elif child_tag == 'Location':
                        for loc_child in child:
                            loc_tag = self.sn(loc_child.tag)
                            if loc_tag == 'Latitude' and loc_child.text:
                                typhoon_data['latitude'] = loc_child.text
                            elif loc_tag == 'Longitude' and loc_child.text:
                                typhoon_data['longitude'] = loc_child.text

        return typhoon_data if len(typhoon_data) > 2 else None
