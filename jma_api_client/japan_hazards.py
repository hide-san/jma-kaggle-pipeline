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
    DESCRIPTION = "Designated river flood forecasts from JMA"
    SUBTITLE = "River flood risk levels and inundation forecasts"
    KEYWORDS = ["jma", "japan", "river", "flood", "forecast"]
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
    DESCRIPTION = "Landslide hazard alerts from JMA"
    SUBTITLE = "Landslide risk warnings and precautions"
    KEYWORDS = ["jma", "japan", "landslide", "hazard", "alert"]
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
