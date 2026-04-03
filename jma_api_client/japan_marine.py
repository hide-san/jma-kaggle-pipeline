"""
Marine and ocean observation data from JMA.

Official Resources:
1. 全般海上警報（定時） (General Marine Warning - Scheduled) - VPZU50
2. 地方潮位情報 (Regional Tidal Information) - VMCJ51
3. 全般潮位情報 (General Tidal Information) - VMCJ50

Source Feed: regular_l.xml / other_l.xml
"""

import xml.etree.ElementTree as ET

import pandas as pd

from .base import JMADatasetBase, register_dataset

__all__ = [
    "GeneralMarineWarning",
    "RegionalTidalInfo",
    "GeneralTidalInfo",
]


@register_dataset
class GeneralMarineWarning(JMADatasetBase):
    """General marine warnings from JMA VPZU50."""

    NAME = "japan-general-marine-warning"
    CSV_FILENAME = "japan_general_marine_warning.csv"
    FEED_NAME = "regular_l.xml"
    TYPE_CODES = ("VPZU50", "VPZU54", "VPZU51")
    MERGE_KEYS = ["event_id"]
    DESCRIPTION = "General marine warnings from JMA"
    SUBTITLE = "Sea state warnings for open ocean and coastal areas"
    KEYWORDS = ["jma", "japan", "marine", "warning", "sea", "ocean"]
    MAX_ENTRIES = 250

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VPZU marine warning XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/meteorology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        warning_data = head_data.copy()

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'Warning':
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Item':
                        for item_child in child:
                            item_tag = self.sn(item_child.tag)
                            if item_tag == 'Kind':
                                for kind_child in item_child:
                                    if self.sn(kind_child.tag) == 'Name' and kind_child.text:
                                        warning_data['warning_type'] = kind_child.text
                                        warning_data['warning_type_en'] = self.translate(kind_child.text)
                            elif item_tag == 'Area':
                                for area_child in item_child:
                                    if self.sn(area_child.tag) == 'Name' and area_child.text:
                                        warning_data['area_name'] = area_child.text
                                        warning_data['area_name_en'] = self.translate(area_child.text)

        return warning_data if len(warning_data) > 2 else None


@register_dataset
class RegionalTidalInfo(JMADatasetBase):
    """Regional tidal information from JMA VMCJ51."""

    NAME = "japan-regional-tidal-information"
    CSV_FILENAME = "japan_regional_tidal_information.csv"
    FEED_NAME = "other_l.xml"
    TYPE_CODES = ("VMCJ51",)
    MERGE_KEYS = ["event_id"]
    DESCRIPTION = "Regional tidal information from JMA"
    SUBTITLE = "Tidal predictions and observations by region"
    KEYWORDS = ["jma", "japan", "tidal", "tide", "ocean", "marine"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VMCJ51 regional tidal information XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/meteorology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        tidal_data = head_data.copy()

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'DateTime' and elem.text:
                tidal_data['forecast_time'] = elem.text

            elif tag == 'Item':
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Area':
                        for area_child in child:
                            if self.sn(area_child.tag) == 'Name' and area_child.text:
                                tidal_data['region_name'] = area_child.text
                                tidal_data['region_name_en'] = self.translate(area_child.text)
                    elif child_tag == 'Kind':
                        for kind_child in child:
                            if self.sn(kind_child.tag) == 'Name' and kind_child.text:
                                tidal_data['tidal_type'] = kind_child.text
                                tidal_data['tidal_type_en'] = self.translate(kind_child.text)

        return tidal_data if len(tidal_data) > 2 else None


@register_dataset
class GeneralTidalInfo(JMADatasetBase):
    """General tidal information from JMA VMCJ50."""

    NAME = "japan-general-tidal-information"
    CSV_FILENAME = "japan_general_tidal_information.csv"
    FEED_NAME = "other_l.xml"
    TYPE_CODES = ("VMCJ50",)
    MERGE_KEYS = ["event_id"]
    DESCRIPTION = "General tidal information from JMA"
    SUBTITLE = "National tidal forecasts and anomaly information"
    KEYWORDS = ["jma", "japan", "tidal", "tide", "coast", "marine"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VMCJ50 general tidal information XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/meteorology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        tidal_data = head_data.copy()

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'DateTime' and elem.text:
                tidal_data['forecast_time'] = elem.text

            elif tag == 'Item':
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Kind':
                        for kind_child in child:
                            if self.sn(kind_child.tag) == 'Name' and kind_child.text:
                                tidal_data['information_type'] = kind_child.text
                                tidal_data['information_type_en'] = self.translate(kind_child.text)

        return tidal_data if len(tidal_data) > 2 else None
