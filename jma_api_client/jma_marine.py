"""
Marine and ocean observation data from JMA.

Official Resources:
1. 全般海上警報（定時） (General Marine Warning - Scheduled) - VPZU50
2. 地方潮位情報 (Regional Tidal Information) - VMCJ51
3. 全般潮位情報 (General Tidal Information) - VMCJ50

Source Feed: regular_l.xml / other_l.xml
"""

import xml.etree.ElementTree as ET

from .base import JMADatasetBase, register_dataset

__all__ = [
    "GeneralMarineWarning",
    "RegionalTidalInfo",
    "GeneralTidalInfo",
]


@register_dataset
class GeneralMarineWarning(JMADatasetBase):
    """General marine warnings from JMA VPZU50."""

    NAME = "jma-general-marine-warning"
    CSV_FILENAME = "jma_general_marine_warning.csv"
    FEED_NAME = "regular_l.xml"
    TYPE_CODES = ("VPZU50", "VPZU54", "VPZU51")
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA General Marine Warning"
    DESCRIPTION = (
        "全般海上警報 — General Marine Warning. "
        "JMA warnings for open ocean and adjacent coastal areas covering strong winds, "
        "high waves, fog, and icing conditions affecting shipping and maritime operations.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, title_en, "
        "warning_type_en, area_name_en\n\n"
        "**Feed:** regular_l.xml | **Type codes:** VPZU50, VPZU51, VPZU54\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 250\n"
        "**Use cases:** maritime safety, shipping route planning, "
        "ocean hazard monitoring, marine meteorology research"
    )
    SUBTITLE = "Open ocean marine warnings with sea state and wind hazard information"
    KEYWORDS = ["japan", "ocean", "sea", "weather", "wind"]
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

    NAME = "jma-regional-tidal-information"
    CSV_FILENAME = "jma_regional_tidal_information.csv"
    FEED_NAME = "other_l.xml"
    TYPE_CODES = ("VMCJ51",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Regional Tidal Information"
    DESCRIPTION = (
        "地方潮位情報 — Regional Tidal Information. "
        "JMA tidal predictions for specified coastal regions including high/low tide times "
        "and heights, used for maritime operations and tsunami baseline monitoring.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, title_en, "
        "forecast_time, region_name_en, tidal_type_en\n\n"
        "**Feed:** other_l.xml | **Type code:** VMCJ51\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** coastal navigation, tsunami detection baseline, "
        "tidal research, port operations planning"
    )
    SUBTITLE = "Tidal predictions with high/low tide times and heights by coastal region"
    KEYWORDS = ["japan", "ocean", "sea", "water", "east-asia"]
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

    NAME = "jma-general-tidal-information"
    CSV_FILENAME = "jma_general_tidal_information.csv"
    FEED_NAME = "other_l.xml"
    TYPE_CODES = ("VMCJ50",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA General Tidal Information"
    DESCRIPTION = (
        "全般潮位情報 — General Tidal Information. "
        "JMA nationwide tidal forecasts and anomaly observations, including tidal anomaly "
        "types and event data used for tsunami detection and marine environmental monitoring.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, title_en, "
        "forecast_time, information_type_en\n\n"
        "**Feed:** other_l.xml | **Type code:** VMCJ50\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** tsunami monitoring, tidal anomaly detection, "
        "sea level research, marine environmental monitoring"
    )
    SUBTITLE = "National tidal forecasts with tidal anomaly observations and event details"
    KEYWORDS = ["japan", "ocean", "sea", "water", "east-asia"]
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
