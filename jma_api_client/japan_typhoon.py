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

    NAME = "japan-typhoon-information"
    CSV_FILENAME = "japan_typhoon_information.csv"
    FEED_NAME = "regular_l.xml"
    TYPE_CODES = ("VPTI50",)
    MERGE_KEYS = ["event_id"]
    DESCRIPTION = "General typhoon information (VPTI50) with current position, central pressure, maximum wind speed, and forecast track. Updated regularly during typhoon seasons."
    SUBTITLE = "Typhoon position, intensity (pressure/wind), and forecast track information"
    KEYWORDS = ["jma", "japan", "typhoon", "tropical", "cyclone", "forecast", "position", "intensity"]
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

    NAME = "japan-typhoon-information-standardized"
    CSV_FILENAME = "japan_typhoon_information_standardized.csv"
    FEED_NAME = "regular_l.xml"
    TYPE_CODES = ("VPTI51",)
    MERGE_KEYS = ["event_id"]
    DESCRIPTION = "Standardized typhoon information (VPTI51) in fixed format for automated processing. Includes pressure, wind speed, and observation times in consistent structure."
    SUBTITLE = "Standardized-format typhoon data with pressure and wind speed"
    KEYWORDS = ["jma", "japan", "typhoon", "standardized", "cyclone", "data-format"]
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

    NAME = "japan-typhoon-information-detailed"
    CSV_FILENAME = "japan_typhoon_information_detailed.csv"
    FEED_NAME = "regular_l.xml"
    TYPE_CODES = ("VPTI52",)
    MERGE_KEYS = ["event_id"]
    DESCRIPTION = "Detailed typhoon information (VPTI52) with extended forecast positions and impacts. Provides comprehensive data for serious typhoon impacts on Japan."
    SUBTITLE = "Detailed typhoon forecasts with extended track and impact predictions"
    KEYWORDS = ["jma", "japan", "typhoon", "detailed", "forecast", "extended", "impacts"]
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
