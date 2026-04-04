"""
JMA informational notices and alerts.

Official Resources:
1. 地震・津波に関するお知らせ (Earthquake/Tsunami Notice) - VZSE40
2. 火山に関するお知らせ (Volcano Notice) - VZVO40

Source Feed: eqvol_l.xml
"""

import xml.etree.ElementTree as ET

import pandas as pd

from .base import JMADatasetBase, register_dataset

__all__ = [
    "EarthquakeTsunamiNotice",
    "VolcanoNotice",
]


@register_dataset
class EarthquakeTsunamiNotice(JMADatasetBase):
    """Earthquake and tsunami informational notices from JMA VZSE40."""

    NAME = "jma-earthquake-and-tsunami-notices"
    CSV_FILENAME = "jma_earthquake_and_tsunami_notices.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VZSE40",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Earthquake & Tsunami Notices"
    DESCRIPTION = (
        "地震・津波に関するお知らせ — Earthquake/Tsunami Notice. "
        "JMA informational notices providing explanations, clarifications, and supplementary "
        "updates about seismic and tsunami events beyond the standard alert messages.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, title_en, "
        "notice_time, notice_content_en, notice_type_en\n\n"
        "**Feed:** eqvol_l.xml | **Type code:** VZSE40\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** seismic event supplementary context, public information, "
        "disaster communication research, JMA advisory tracking"
    )
    SUBTITLE = "Explanatory notices and clarifications about earthquake/tsunami events"
    KEYWORDS = ["japan", "earthquake", "natural-disaster", "government", "east-asia"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VZSE40 earthquake/tsunami notice XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/seismology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        notice_data = head_data.copy()

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'DateTime' and elem.text:
                notice_data['notice_time'] = elem.text

            elif tag == 'Text' and elem.text:
                notice_data['notice_content'] = elem.text.strip()
                notice_data['notice_content_en'] = self.translate(elem.text.strip())

            elif tag == 'Item':
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Kind':
                        for kind_child in child:
                            if self.sn(kind_child.tag) == 'Name' and kind_child.text:
                                notice_data['notice_type'] = kind_child.text
                                notice_data['notice_type_en'] = self.translate(kind_child.text)

        return notice_data if len(notice_data) > 2 else None


@register_dataset
class VolcanoNotice(JMADatasetBase):
    """Volcano informational notices from JMA VZVO40."""

    NAME = "jma-volcano-notices"
    CSV_FILENAME = "jma_volcano_notices.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VZVO40",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Volcano Notices"
    DESCRIPTION = (
        "火山に関するお知らせ — Volcano Notice. "
        "JMA informational notices and announcements about volcanic activity, providing "
        "supplementary explanations and updates beyond standard volcanic alert messages.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, title_en, "
        "notice_time, notice_content_en, volcano_name_en\n\n"
        "**Feed:** eqvol_l.xml | **Type code:** VZVO40\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** volcanic activity supplementary context, public advisories, "
        "volcano monitoring, disaster communication research"
    )
    SUBTITLE = "Explanatory notices and announcements about volcanic activity"
    KEYWORDS = ["japan", "natural-disaster", "environment", "government", "east-asia"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VZVO40 volcano notice XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/volcanology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        notice_data = head_data.copy()

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'DateTime' and elem.text:
                notice_data['notice_time'] = elem.text

            elif tag == 'Text' and elem.text:
                notice_data['notice_content'] = elem.text.strip()
                notice_data['notice_content_en'] = self.translate(elem.text.strip())

            elif tag == 'VolcanoInfo':
                for child in elem:
                    if self.sn(child.tag) == 'Item':
                        for item_child in child:
                            item_tag = self.sn(item_child.tag)
                            if item_tag == 'Areas':
                                for area in item_child:
                                    if self.sn(area.tag) == 'Area':
                                        for area_child in area:
                                            if self.sn(area_child.tag) == 'Name' and area_child.text:
                                                notice_data['volcano_name'] = area_child.text
                                                notice_data['volcano_name_en'] = self.translate(area_child.text)

        return notice_data if len(notice_data) > 2 else None
