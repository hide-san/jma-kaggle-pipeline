"""
Extended weather forecasts and seasonal predictions from JMA.

Official Resources:
1. 全般１か月予報・３か月予報・暖寒候期予報 (General Monthly/Seasonal Forecasts) - VPZK50
2. 地方１か月予報・３か月予報・暖寒候期予報 (Regional Monthly/Seasonal Forecasts) - VPCK50

Source Feed: regular_l.xml
"""

import xml.etree.ElementTree as ET

import pandas as pd

from .base import JMADatasetBase, register_dataset

__all__ = [
    "GeneralSeasonalForecast",
    "RegionalSeasonalForecast",
]


@register_dataset
class GeneralSeasonalForecast(JMADatasetBase):
    """General monthly and seasonal forecasts from JMA VPZK50."""

    NAME = "japan-general-seasonal-forecast"
    CSV_FILENAME = "japan_general_seasonal_forecast.csv"
    FEED_NAME = "regular_l.xml"
    TYPE_CODES = ("VPZK50",)
    MERGE_KEYS = ["event_id"]
    DESCRIPTION = "General seasonal forecasts from JMA"
    SUBTITLE = "1-month, 3-month, and seasonal temperature/precipitation forecasts"
    KEYWORDS = ["jma", "japan", "forecast", "seasonal", "monthly"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VPZK50 general seasonal forecast XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/meteorology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        forecast_data = head_data.copy()

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'DateTime' and elem.text:
                forecast_data['forecast_issued_time'] = elem.text

            elif tag == 'Item':
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Kind':
                        for kind_child in child:
                            if self.sn(kind_child.tag) == 'Name' and kind_child.text:
                                forecast_data['forecast_type'] = kind_child.text
                                forecast_data['forecast_type_en'] = self.translate(kind_child.text)
                    elif child_tag == 'Period' and child.text:
                        forecast_data['forecast_period'] = child.text

        return forecast_data if len(forecast_data) > 2 else None


@register_dataset
class RegionalSeasonalForecast(JMADatasetBase):
    """Regional monthly and seasonal forecasts from JMA VPCK50."""

    NAME = "japan-regional-seasonal-forecast"
    CSV_FILENAME = "japan_regional_seasonal_forecast.csv"
    FEED_NAME = "regular_l.xml"
    TYPE_CODES = ("VPCK50",)
    MERGE_KEYS = ["event_id"]
    DESCRIPTION = "Regional seasonal forecasts from JMA"
    SUBTITLE = "Regional 1-month, 3-month, and seasonal predictions"
    KEYWORDS = ["jma", "japan", "regional", "forecast", "seasonal"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VPCK50 regional seasonal forecast XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/meteorology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        forecast_data = head_data.copy()

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'DateTime' and elem.text:
                forecast_data['forecast_issued_time'] = elem.text

            elif tag == 'Item':
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Kind':
                        for kind_child in child:
                            if self.sn(kind_child.tag) == 'Name' and kind_child.text:
                                forecast_data['forecast_type'] = kind_child.text
                                forecast_data['forecast_type_en'] = self.translate(kind_child.text)
                    elif child_tag == 'Area':
                        for area_child in child:
                            if self.sn(area_child.tag) == 'Name' and area_child.text:
                                forecast_data['region_name'] = area_child.text
                                forecast_data['region_name_en'] = self.translate(area_child.text)
                    elif child_tag == 'Period' and child.text:
                        forecast_data['forecast_period'] = child.text

        return forecast_data if len(forecast_data) > 2 else None
