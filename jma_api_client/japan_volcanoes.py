"""
Volcano data from JMA.

Official Resources:
1. 火山の状況に関する解説情報 (Volcano Status Explanation) - VFVO51
2. 降灰予報 (Volcanic Ash Forecast) - VFVO53
3. 噴火警報・予報 (Eruption Warning/Forecast) - VFVO50
4. 噴火速報 (Eruption Flash Report) - VFVO56

Source Feed: eqvol_l.xml
"""

import json
import xml.etree.ElementTree as ET

import pandas as pd

from .base import JMADatasetBase, register_dataset

__all__ = [
    "VolcanoStatusExplanation",
    "VolcanicAshForecast",
    "EruptionWarning",
    "EruptionFlashReport",
    "EruptionObservation",
    "EstimatedPlumeDirection",
]


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


@register_dataset
class EruptionWarning(JMADatasetBase):
    """Eruption warning and forecast data from JMA VFVO50."""

    NAME = "japan-eruption-warning"
    CSV_FILENAME = "japan_eruption_warning.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VFVO50",)
    MERGE_KEYS = ["event_id"]
    DESCRIPTION = "Eruption warnings and forecasts from JMA"
    SUBTITLE = "Alert levels and warnings for volcanic activity"
    KEYWORDS = ["jma", "japan", "volcano", "eruption", "warning"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VFVO50 eruption warning XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/volcanology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        warning_data = head_data.copy()

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'VolcanoInfo':
                for child in elem:
                    if self.sn(child.tag) == 'Item':
                        for item_child in child:
                            item_tag = self.sn(item_child.tag)
                            if item_tag == 'Kind':
                                for kind_child in item_child:
                                    kind_tag = self.sn(kind_child.tag)
                                    if kind_tag == 'Name' and kind_child.text:
                                        warning_data['warning_type'] = kind_child.text
                                        warning_data['warning_type_en'] = self.translate(kind_child.text)
                                    elif kind_tag == 'Code' and kind_child.text:
                                        warning_data['warning_code'] = kind_child.text

                        for item_child in child:
                            if self.sn(item_child.tag) == 'Areas':
                                for area in item_child:
                                    if self.sn(area.tag) == 'Area':
                                        for area_child in area:
                                            if self.sn(area_child.tag) == 'Name' and area_child.text:
                                                warning_data['volcano_name'] = area_child.text
                                                warning_data['volcano_name_en'] = self.translate(area_child.text)

        return warning_data if len(warning_data) > 2 else None


@register_dataset
class EruptionFlashReport(JMADatasetBase):
    """Eruption flash reports from JMA VFVO56."""

    NAME = "japan-eruption-flash-report"
    CSV_FILENAME = "japan_eruption_flash_report.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VFVO56",)
    MERGE_KEYS = ["event_id"]
    DESCRIPTION = "Flash reports of volcanic eruptions from JMA"
    SUBTITLE = "Rapid notifications of ongoing or imminent eruptions"
    KEYWORDS = ["jma", "japan", "volcano", "eruption", "flash", "report"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VFVO56 eruption flash report XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/volcanology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        report_data = head_data.copy()

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'Eruption':
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'DateTime' and child.text:
                        report_data['eruption_time'] = child.text
                    elif child_tag == 'Item':
                        for item_child in child:
                            item_tag = self.sn(item_child.tag)
                            if item_tag == 'Kind':
                                for kind_child in item_child:
                                    if self.sn(kind_child.tag) == 'Name' and kind_child.text:
                                        report_data['eruption_type'] = kind_child.text
                                        report_data['eruption_type_en'] = self.translate(kind_child.text)
                            elif item_tag == 'Areas':
                                for area in item_child:
                                    if self.sn(area.tag) == 'Area':
                                        for area_child in area:
                                            if self.sn(area_child.tag) == 'Name' and area_child.text:
                                                report_data['volcano_name'] = area_child.text
                                                report_data['volcano_name_en'] = self.translate(area_child.text)

        return report_data if len(report_data) > 2 else None


@register_dataset
class EruptionObservation(JMADatasetBase):
    """Eruption-related volcanic observation reports from JMA VFVO52."""

    NAME = "japan-eruption-observation"
    CSV_FILENAME = "japan_eruption_observation.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VFVO52",)
    MERGE_KEYS = ["event_id"]
    DESCRIPTION = "Volcanic observation reports related to eruptions from JMA"
    SUBTITLE = "Detailed observations of volcanic activity and eruptions"
    KEYWORDS = ["jma", "japan", "volcano", "eruption", "observation"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VFVO52 eruption observation report XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/volcanology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        obs_data = head_data.copy()

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'DateTime' and elem.text:
                obs_data['observation_time'] = elem.text

            elif tag == 'VolcanoInfo':
                for child in elem:
                    if self.sn(child.tag) == 'Item':
                        for item_child in child:
                            item_tag = self.sn(item_child.tag)
                            if item_tag == 'Kind':
                                for kind_child in item_child:
                                    if self.sn(kind_child.tag) == 'Name' and kind_child.text:
                                        obs_data['observation_type'] = kind_child.text
                                        obs_data['observation_type_en'] = self.translate(kind_child.text)
                            elif item_tag == 'Areas':
                                for area in item_child:
                                    if self.sn(area.tag) == 'Area':
                                        for area_child in area:
                                            if self.sn(area_child.tag) == 'Name' and area_child.text:
                                                obs_data['volcano_name'] = area_child.text
                                                obs_data['volcano_name_en'] = self.translate(area_child.text)

        return obs_data if len(obs_data) > 2 else None


@register_dataset
class EstimatedPlumeDirection(JMADatasetBase):
    """Estimated volcanic plume flow direction from JMA VFVO60."""

    NAME = "japan-estimated-plume-direction"
    CSV_FILENAME = "japan_estimated_plume_direction.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VFVO60",)
    MERGE_KEYS = ["event_id"]
    DESCRIPTION = "Estimated volcanic plume flow direction from JMA"
    SUBTITLE = "Predicted ash plume movement based on atmospheric conditions"
    KEYWORDS = ["jma", "japan", "volcano", "plume", "ash", "direction"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VFVO60 estimated plume direction XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/volcanology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        plume_data = head_data.copy()
        directions = []

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'PlumeDirection':
                dir_dict = {}
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'DateTime' and child.text:
                        dir_dict['forecast_time'] = child.text
                    elif child_tag == 'Direction' and child.text:
                        dir_dict['direction'] = child.text
                        dir_dict['direction_en'] = self.translate(child.text)

                if dir_dict:
                    directions.append(dir_dict)

            elif tag == 'Volcano':
                for child in elem:
                    if self.sn(child.tag) == 'Name' and child.text:
                        plume_data['volcano_name'] = child.text
                        plume_data['volcano_name_en'] = self.translate(child.text)

        if directions:
            plume_data['plume_directions_json'] = json.dumps(directions, ensure_ascii=False)

        return plume_data if len(plume_data) > 2 else None


# For backwards compatibility
def fetch_volcano_status() -> pd.DataFrame:
    """Legacy function wrapper."""
    return VolcanoStatusExplanation().fetch()


def fetch_volcanic_ash_forecasts() -> pd.DataFrame:
    """Legacy function wrapper."""
    return VolcanicAshForecast().fetch()
