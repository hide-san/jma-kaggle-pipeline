"""
Natural hazard warning data from JMA.

Official Resources:
1. 指定河川洪水予報 (Designated River Flood Forecast) - VXKO50-89
2. 土砂災害警戒情報 (Landslide Hazard Alert) - VXWW50

Source Feed: regular_l.xml
"""

import xml.etree.ElementTree as ET

import pandas as pd

from .base import JMADatasetBase, register_dataset

__all__ = [
    "RiverFloodForecast",
    "LandslideHazardAlert",
]


@register_dataset
class RiverFloodForecast(JMADatasetBase):
    """Designated river flood forecasts from JMA VXKO50-89."""

    NAME = "japan-river-flood-forecast"
    CSV_FILENAME = "japan_river_flood_forecast.csv"
    FEED_NAME = "regular_l.xml"
    TYPE_CODES = tuple(f"VXKO{i:02d}" for i in range(50, 90))
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Designated River Flood Forecasts (VXKO50-89)"
    DESCRIPTION = (
        "指定河川洪水予報 — Designated River Flood Forecast. "
        "JMA flood forecasts for 63 major designated rivers in Japan, providing flood stage "
        "predictions, peak timing, and affected prefectures for emergency preparedness.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, title_en, "
        "forecast_time, river_name_en, region_name_en\n\n"
        "**Feed:** regular_l.xml | **Type codes:** VXKO50–VXKO89\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 500\n"
        "**Use cases:** flood risk management, river monitoring, "
        "emergency evacuation planning, hydrological research"
    )
    SUBTITLE = "River flood forecasts by designated river with stage predictions and timing"
    KEYWORDS = ["jma", "japan", "river", "flood", "forecast", "hazard", "emergency"]
    MAX_ENTRIES = 500

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VXKO river flood forecast XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/meteorology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        flood_data = head_data.copy()

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'DateTime' and elem.text:
                flood_data['forecast_time'] = elem.text

            elif tag == 'Item':
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Kind':
                        for kind_child in child:
                            if self.sn(kind_child.tag) == 'Name' and kind_child.text:
                                flood_data['river_name'] = kind_child.text
                                flood_data['river_name_en'] = self.translate(kind_child.text)
                    elif child_tag == 'Area':
                        for area_child in child:
                            if self.sn(area_child.tag) == 'Name' and area_child.text:
                                flood_data['region_name'] = area_child.text
                                flood_data['region_name_en'] = self.translate(area_child.text)

        return flood_data if len(flood_data) > 2 else None


@register_dataset
class LandslideHazardAlert(JMADatasetBase):
    """Landslide hazard alerts from JMA VXWW50."""

    NAME = "japan-landslide-hazard-alert"
    CSV_FILENAME = "japan_landslide_hazard_alert.csv"
    FEED_NAME = "regular_l.xml"
    TYPE_CODES = ("VXWW50",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Landslide Hazard Alerts (VXWW50)"
    DESCRIPTION = (
        "土砂災害警戒情報 — Landslide Hazard Alert. "
        "JMA alerts issued when accumulated rainfall exceeds thresholds for dangerous "
        "landslide conditions, specifying hazard types and affected municipalities.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, title_en, "
        "alert_time, hazard_type_en, region_name_en, region_code\n\n"
        "**Feed:** regular_l.xml | **Type code:** VXWW50\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** landslide risk monitoring, rainfall-triggered hazard research, "
        "evacuation planning, disaster prevention"
    )
    SUBTITLE = "Landslide risk alerts with trigger conditions and affected areas"
    KEYWORDS = ["jma", "japan", "landslide", "hazard", "alert", "rainfall", "emergency"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VXWW50 landslide hazard alert XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/meteorology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        landslide_data = head_data.copy()

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'DateTime' and elem.text:
                landslide_data['alert_time'] = elem.text

            elif tag == 'Item':
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Kind':
                        for kind_child in child:
                            if self.sn(kind_child.tag) == 'Name' and kind_child.text:
                                landslide_data['hazard_type'] = kind_child.text
                                landslide_data['hazard_type_en'] = self.translate(kind_child.text)
                    elif child_tag == 'Area':
                        for area_child in child:
                            area_tag = self.sn(area_child.tag)
                            if area_tag == 'Name' and area_child.text:
                                landslide_data['region_name'] = area_child.text
                                landslide_data['region_name_en'] = self.translate(area_child.text)
                            elif area_tag == 'Code' and area_child.text:
                                landslide_data['region_code'] = area_child.text

        return landslide_data if len(landslide_data) > 2 else None
