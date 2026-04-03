"""
Volcano data from JMA.

Official Resources:
1. 火山の状況に関する解説情報 (Volcano Status Explanation) - Data Type Code: VFVO51
2. 降灰予報 (Volcanic Ash Forecast) - Data Type Code: VFVO53

Source Feed: eqvol_l.xml
"""

import xml.etree.ElementTree as ET

import pandas as pd

from .base import JMADatasetBase, register_dataset

__all__ = ["VolcanoStatusExplanation", "VolcanicAshForecast"]


@register_dataset
class VolcanoStatusExplanation(JMADatasetBase):
    """Volcano status and alert level information from JMA VFVO51."""

    NAME = "japan-volcano-status-explanation"
    CSV_FILENAME = "japan_volcano_status_explanation.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VFVO51",)
    MERGE_KEYS = ["event_id"]
    DESCRIPTION = "Volcano status explanation with alert levels from JMA"
    SUBTITLE = "Alert levels and activity summaries for active volcanoes"
    KEYWORDS = ["jma", "japan", "volcano", "alert", "volcanology"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VFVO51 volcano status XML."""
        # Extract header info
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        # Find body
        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/volcanology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        volcano_data = head_data.copy()

        # Extract volcano info and alert level
        for elem in body.iter():
            tag = self.sn(elem.tag)

            # Get volcano name and alert level from VolcanoInfo
            if tag == 'VolcanoInfo':
                for child in elem:
                    if self.sn(child.tag) == 'Item':
                        # Extract alert level info
                        for item_child in child:
                            if self.sn(item_child.tag) == 'Kind':
                                for kind_child in item_child:
                                    kind_tag = self.sn(kind_child.tag)
                                    if kind_tag == 'Name' and kind_child.text:
                                        volcano_data['alert_level'] = kind_child.text
                                        volcano_data['alert_level_en'] = self.translate(kind_child.text)
                                    elif kind_tag == 'Code' and kind_child.text:
                                        volcano_data['alert_level_code'] = kind_child.text
                                    elif kind_tag == 'Condition' and kind_child.text:
                                        volcano_data['alert_condition'] = kind_child.text
                                        volcano_data['alert_condition_en'] = self.translate(kind_child.text)

                        # Extract volcano name from Areas
                        for item_child in child:
                            if self.sn(item_child.tag) == 'Areas':
                                for area in item_child:
                                    if self.sn(area.tag) == 'Area':
                                        for area_child in area:
                                            if self.sn(area_child.tag) == 'Name' and area_child.text:
                                                volcano_data['volcano_name'] = area_child.text
                                                volcano_data['volcano_name_en'] = self.translate(area_child.text)
                                                break

            # Get activity and prevention summaries from VolcanoInfoContent
            elif tag == 'VolcanoInfoContent':
                for content_child in elem:
                    content_tag = self.sn(content_child.tag)
                    if content_tag == 'VolcanoActivity' and content_child.text:
                        activity_text = content_child.text.strip()
                        volcano_data['activity_summary'] = activity_text
                        volcano_data['activity_summary_en'] = self.translate(activity_text)
                    elif content_tag == 'VolcanoPrevention' and content_child.text:
                        prevention_text = content_child.text.strip()
                        volcano_data['prevention_summary'] = prevention_text
                        volcano_data['prevention_summary_en'] = self.translate(prevention_text)

        return volcano_data if len(volcano_data) > 2 else None


@register_dataset
class VolcanicAshForecast(JMADatasetBase):
    """Volcanic ash forecast data from JMA VFVO53."""

    NAME = "japan-volcanic-ash-forecast"
    CSV_FILENAME = "japan_volcanic_ash_forecast.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VFVO53",)
    MERGE_KEYS = ["event_id"]
    DESCRIPTION = "Volcanic ash forecasts with affected areas from JMA"
    SUBTITLE = "6-hour window volcanic ash predictions and affected regions"
    KEYWORDS = ["jma", "japan", "volcano", "ash", "forecast"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VFVO53 volcanic ash forecast XML."""
        # Extract header info
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        # Find body
        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/volcanology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        ash_data = head_data.copy()

        # Extract volcano name from VolcanoInfo
        for elem in body.iter():
            tag = self.sn(elem.tag)
            if tag == 'Area':
                for child in elem:
                    if self.sn(child.tag) == 'Name' and child.text:
                        ash_data['volcano_name'] = child.text
                        ash_data['volcano_name_en'] = self.translate(child.text)
                        break

        # Find all AshInfo entries (should be up to 6 time windows)
        ash_infos = []
        for elem in body.iter():
            tag = self.sn(elem.tag)
            if tag == 'AshInfo':
                ash_infos.append(elem)

        # Process up to 6 forecast windows
        for window_idx, ash_info in enumerate(ash_infos[:6], 1):
            start_time = None
            end_time = None
            affected_areas = []

            for child in ash_info:
                child_tag = self.sn(child.tag)
                if child_tag == 'StartTime' and child.text:
                    start_time = child.text
                elif child_tag == 'EndTime' and child.text:
                    end_time = child.text
                elif child_tag == 'Item':
                    # Extract affected areas
                    for item_child in child:
                        if self.sn(item_child.tag) == 'Areas':
                            for area in item_child:
                                if self.sn(area.tag) == 'Area':
                                    for area_child in area:
                                        if self.sn(area_child.tag) == 'Name' and area_child.text:
                                            affected_areas.append(area_child.text)

            # Store window data (keep original and add English translation)
            ash_data[f'window_{window_idx}_start'] = start_time
            ash_data[f'window_{window_idx}_end'] = end_time
            ash_data[f'window_{window_idx}_areas'] = ', '.join(affected_areas) if affected_areas else ''

            # Add English translations
            translated_areas = [self.translate(area) for area in affected_areas]
            ash_data[f'window_{window_idx}_areas_en'] = ', '.join(translated_areas) if translated_areas else ''

        return ash_data if len(ash_data) > 2 else None


# For backwards compatibility
def fetch_volcano_status() -> pd.DataFrame:
    """Legacy function wrapper."""
    return VolcanoStatusExplanation().fetch()


def fetch_volcanic_ash_forecasts() -> pd.DataFrame:
    """Legacy function wrapper."""
    return VolcanicAshForecast().fetch()
