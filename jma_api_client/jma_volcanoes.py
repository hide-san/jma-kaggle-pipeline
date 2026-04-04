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
import re
import xml.etree.ElementTree as ET

import pandas as pd

from .base import JMADatasetBase, register_dataset


def _parse_volcano_coordinate(coord_str: str) -> tuple[float | None, float | None, int | None]:
    """
    Parse JMA volcano coordinate string "+DDMM.MM+DDDMM.MM+ELEVm/" to
    (latitude_decimal, longitude_decimal, elevation_m).
    Format is degrees-minutes: +2938.30 = 29°38.30'N.
    """
    try:
        coord_str = coord_str.strip().rstrip('/')
        parts = re.findall(r'[+-]\d+\.?\d*', coord_str)
        if len(parts) < 2:
            return None, None, None
        lat_raw = float(parts[0])
        lon_raw = float(parts[1])
        lat_deg = int(abs(lat_raw) / 100)
        lat_min = abs(lat_raw) % 100
        lat = (lat_deg + lat_min / 60) * (1 if lat_raw >= 0 else -1)
        lon_deg = int(abs(lon_raw) / 100)
        lon_min = abs(lon_raw) % 100
        lon = (lon_deg + lon_min / 60) * (1 if lon_raw >= 0 else -1)
        elev = int(float(parts[2])) if len(parts) >= 3 else None
        return round(lat, 5), round(lon, 5), elev
    except (ValueError, IndexError):
        return None, None, None

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

    NAME = "jma-volcanic-situation-explanation"
    CSV_FILENAME = "jma_volcanic_situation_explanation.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VFVO51",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Volcanic Situation Explanation"
    DESCRIPTION = (
        "火山の状況に関する解説情報 — Volcano Status Explanation. "
        "Detailed JMA explanations of current volcanic activity with alert levels (1–5) "
        "for all monitored volcanoes in Japan.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, volcano_name_en, "
        "volcano_code, volcano_latitude, volcano_longitude, volcano_elevation_m, "
        "alert_level_en, alert_level_code, alert_condition_en, "
        "last_alert_level_en, last_alert_level_code, "
        "volcano_headline, activity_summary_en, prevention_summary_en, next_advisory\n\n"
        "**Feed:** eqvol_l.xml | **Type code:** VFVO51\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** volcano hazard monitoring, alert level trend analysis, "
        "public safety research, emergency preparedness"
    )
    SUBTITLE = "Volcano alert level (1-5) explanations with activity and prevention summaries"
    KEYWORDS = ["japan", "natural-disaster", "disaster", "environment", "public-safety"]
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

            # Get volcano name, alert level, and coordinates from VolcanoInfo
            if tag == 'VolcanoInfo':
                for child in elem:
                    if self.sn(child.tag) == 'Item':
                        for item_child in child:
                            item_tag = self.sn(item_child.tag)
                            if item_tag == 'Kind':
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
                            elif item_tag == 'LastKind':
                                for lk_child in item_child:
                                    lk_tag = self.sn(lk_child.tag)
                                    if lk_tag == 'Name' and lk_child.text:
                                        volcano_data['last_alert_level'] = lk_child.text
                                        volcano_data['last_alert_level_en'] = self.translate(lk_child.text)
                                    elif lk_tag == 'Code' and lk_child.text:
                                        volcano_data['last_alert_level_code'] = lk_child.text
                            elif item_tag == 'Areas':
                                for area in item_child:
                                    if self.sn(area.tag) == 'Area':
                                        for area_child in area:
                                            ac_tag = self.sn(area_child.tag)
                                            if ac_tag == 'Name' and area_child.text:
                                                volcano_data['volcano_name'] = area_child.text
                                                volcano_data['volcano_name_en'] = self.translate(area_child.text)
                                            elif ac_tag == 'Code' and area_child.text:
                                                volcano_data['volcano_code'] = area_child.text
                                            elif ac_tag == 'Coordinate' and area_child.text:
                                                lat, lon, elev = _parse_volcano_coordinate(area_child.text)
                                                if lat is not None:
                                                    volcano_data['volcano_latitude'] = lat
                                                    volcano_data['volcano_longitude'] = lon
                                                    if elev is not None:
                                                        volcano_data['volcano_elevation_m'] = elev

            # Get activity, prevention summaries, and next advisory from VolcanoInfoContent
            elif tag == 'VolcanoInfoContent':
                for content_child in elem:
                    content_tag = self.sn(content_child.tag)
                    if content_tag == 'VolcanoHeadline' and content_child.text:
                        volcano_data['volcano_headline'] = content_child.text.strip()
                    elif content_tag == 'VolcanoActivity' and content_child.text:
                        activity_text = content_child.text.strip()
                        volcano_data['activity_summary'] = activity_text
                        volcano_data['activity_summary_en'] = self.translate(activity_text)
                    elif content_tag == 'VolcanoPrevention' and content_child.text:
                        prevention_text = content_child.text.strip()
                        volcano_data['prevention_summary'] = prevention_text
                        volcano_data['prevention_summary_en'] = self.translate(prevention_text)
                    elif content_tag == 'NextAdvisory' and content_child.text:
                        volcano_data['next_advisory'] = content_child.text.strip()

        return volcano_data if len(volcano_data) > 2 else None


@register_dataset
class VolcanicAshForecast(JMADatasetBase):
    """Volcanic ash forecast data from JMA VFVO53."""

    NAME = "jma-volcanic-ash-forecast"
    CSV_FILENAME = "jma_volcanic_ash_forecast.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VFVO53",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Volcanic Ash Forecast"
    DESCRIPTION = (
        "降灰予報 — Volcanic Ash Forecast. "
        "6-hour window ash dispersion forecasts from JMA predicting fallout areas "
        "and affected regions following eruptions, for aviation and public safety.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, volcano_name_en, "
        "volcano_code, valid_datetime, "
        "window_1_start, window_1_end, window_1_areas_en, window_2_start ... window_6_end\n\n"
        "**Feed:** eqvol_l.xml | **Type code:** VFVO53\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** aviation safety, ashfall impact assessment, "
        "emergency preparedness, air quality forecasting"
    )
    SUBTITLE = "6-hour window volcanic ash dispersion forecasts with affected regions and dates"
    KEYWORDS = ["japan", "natural-disaster", "aviation", "environment", "east-asia"]
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

        # Extract valid_datetime from Head
        head = root.find('.//{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Head')
        if head is not None:
            for elem in head.iter():
                if self.sn(elem.tag) == 'ValidDateTime' and elem.text:
                    ash_data['valid_datetime'] = elem.text

        # Extract volcano name and code from VolcanoInfo
        for elem in body.iter():
            tag = self.sn(elem.tag)
            if tag == 'Area':
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Name' and child.text:
                        ash_data['volcano_name'] = child.text
                        ash_data['volcano_name_en'] = self.translate(child.text)
                    elif child_tag == 'Code' and child.text:
                        ash_data['volcano_code'] = child.text
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

    NAME = "jma-eruption-warning-and-forecast"
    CSV_FILENAME = "jma_eruption_warning_and_forecast.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VFVO50",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Eruption Warning and Forecast"
    DESCRIPTION = (
        "噴火警報・予報 — Eruption Warning/Forecast. "
        "JMA eruption warnings and forecasts with alert levels (1–5) for Japanese volcanoes, "
        "indicating danger zones and expected hazards such as ash, pyroclastic flows, and lahars.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, volcano_name_en, "
        "warning_type_en, warning_code\n\n"
        "**Feed:** eqvol_l.xml | **Type code:** VFVO50\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** volcanic hazard monitoring, evacuation planning, "
        "alert level trend analysis, disaster risk research"
    )
    SUBTITLE = "Eruption warning levels (1-5) with expected volcanic hazards and impact areas"
    KEYWORDS = ["japan", "natural-disaster", "disaster", "public-safety", "east-asia"]
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
                                            ac_tag = self.sn(area_child.tag)
                                            if ac_tag == 'Name' and area_child.text:
                                                warning_data['volcano_name'] = area_child.text
                                                warning_data['volcano_name_en'] = self.translate(area_child.text)
                                            elif ac_tag == 'Code' and area_child.text:
                                                warning_data['volcano_code'] = area_child.text

        return warning_data if len(warning_data) > 2 else None


@register_dataset
class EruptionFlashReport(JMADatasetBase):
    """Eruption flash reports from JMA VFVO56."""

    NAME = "jma-eruption-flash-report"
    CSV_FILENAME = "jma_eruption_flash_report.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VFVO56",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Eruption Flash Report"
    DESCRIPTION = (
        "噴火速報 — Eruption Flash Report. "
        "Rapid JMA notifications issued within minutes of confirmed eruptions, "
        "providing eruption time, type, and affected volcano for immediate emergency response.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, volcano_name_en, "
        "eruption_time, eruption_type_en\n\n"
        "**Feed:** eqvol_l.xml | **Type code:** VFVO56\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** eruption event detection, emergency response, "
        "volcanic activity timelines, hazard research"
    )
    SUBTITLE = "Rapid eruption notifications with eruption type and affected volcano"
    KEYWORDS = ["japan", "natural-disaster", "disaster", "alerts", "east-asia"]
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
                                            ac_tag = self.sn(area_child.tag)
                                            if ac_tag == 'Name' and area_child.text:
                                                report_data['volcano_name'] = area_child.text
                                                report_data['volcano_name_en'] = self.translate(area_child.text)
                                            elif ac_tag == 'Code' and area_child.text:
                                                report_data['volcano_code'] = area_child.text

        return report_data if len(report_data) > 2 else None


@register_dataset
class EruptionObservation(JMADatasetBase):
    """Eruption-related volcanic observation reports from JMA VFVO52."""

    NAME = "jma-volcanic-eruption-observation-report"
    CSV_FILENAME = "jma_volcanic_eruption_observation_report.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VFVO52",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Volcanic Eruption Observation Report"
    DESCRIPTION = (
        "噴火に関する火山観測報 — Eruption-related Volcanic Observation Report. "
        "Detailed JMA ground and satellite observation records of volcanic activity, "
        "including eruption phenomena, plume height, and eruptive characteristics.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, volcano_name_en, "
        "volcano_code, volcano_latitude, volcano_longitude, volcano_elevation_m, "
        "crater_name, crater_latitude, crater_longitude, crater_elevation_m, "
        "observation_time, observation_type_en, "
        "plume_height_above_crater_m, plume_height_above_sea_level_ft, plume_direction_en, "
        "other_observation\n\n"
        "**Feed:** eqvol_l.xml | **Type code:** VFVO52\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** volcanic activity monitoring, eruption characterization, "
        "scientific research, geophysical studies"
    )
    SUBTITLE = "Detailed eruption observations with activity characteristics and plume data"
    KEYWORDS = ["japan", "natural-disaster", "environment", "remote-sensing", "east-asia"]
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

            if tag == 'EventDateTime' and elem.text:
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
                                            ac_tag = self.sn(area_child.tag)
                                            if ac_tag == 'Name' and area_child.text:
                                                obs_data['volcano_name'] = area_child.text
                                                obs_data['volcano_name_en'] = self.translate(area_child.text)
                                            elif ac_tag == 'Code' and area_child.text:
                                                obs_data['volcano_code'] = area_child.text
                                            elif ac_tag == 'Coordinate' and area_child.text:
                                                lat, lon, elev = _parse_volcano_coordinate(area_child.text)
                                                if lat is not None:
                                                    obs_data['volcano_latitude'] = lat
                                                    obs_data['volcano_longitude'] = lon
                                                    if elev is not None:
                                                        obs_data['volcano_elevation_m'] = elev
                                            elif ac_tag == 'CraterName' and area_child.text:
                                                obs_data['crater_name'] = area_child.text
                                            elif ac_tag == 'CraterCoordinate' and area_child.text:
                                                lat, lon, elev = _parse_volcano_coordinate(area_child.text)
                                                if lat is not None:
                                                    obs_data['crater_latitude'] = lat
                                                    obs_data['crater_longitude'] = lon
                                                    if elev is not None:
                                                        obs_data['crater_elevation_m'] = elev

            elif tag == 'ColorPlume':
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'PlumeHeightAboveCrater' and child.text:
                        try:
                            obs_data['plume_height_above_crater_m'] = int(child.text)
                        except ValueError:
                            obs_data['plume_height_above_crater_m'] = child.text
                    elif child_tag == 'PlumeHeightAboveSeaLevel' and child.text:
                        try:
                            obs_data['plume_height_above_sea_level_ft'] = int(child.text)
                        except ValueError:
                            obs_data['plume_height_above_sea_level_ft'] = child.text
                    elif child_tag == 'PlumeDirection' and child.text:
                        obs_data['plume_direction'] = child.text
                        obs_data['plume_direction_en'] = self.translate(child.text)

            elif tag == 'OtherObservation' and elem.text:
                obs_data['other_observation'] = elem.text.strip()

        return obs_data if len(obs_data) > 2 else None


@register_dataset
class EstimatedPlumeDirection(JMADatasetBase):
    """Estimated volcanic plume flow direction from JMA VFVO60."""

    NAME = "jma-estimated-plume-direction-report"
    CSV_FILENAME = "jma_estimated_plume_direction_report.csv"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VFVO60",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Estimated Plume Direction Report"
    DESCRIPTION = (
        "火山プルームの移流方向 — Estimated Volcanic Plume Direction. "
        "JMA predictions of volcanic ash plume dispersal direction based on wind fields "
        "and atmospheric modeling, with multiple time-stamped directional forecasts.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, volcano_name_en, "
        "plume_directions_json (list of {forecast_time, direction_en})\n\n"
        "**Feed:** eqvol_l.xml | **Type code:** VFVO60\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** aviation ash avoidance, ashfall trajectory modeling, "
        "atmospheric dispersion research, public safety"
    )
    SUBTITLE = "Volcanic ash plume direction forecasts with time-stamped directional predictions"
    KEYWORDS = ["japan", "natural-disaster", "aviation", "wind", "east-asia"]
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
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Name' and child.text:
                        plume_data['volcano_name'] = child.text
                        plume_data['volcano_name_en'] = self.translate(child.text)
                    elif child_tag == 'Code' and child.text:
                        plume_data['volcano_code'] = child.text

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
