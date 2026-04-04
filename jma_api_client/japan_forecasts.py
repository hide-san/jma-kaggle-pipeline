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
    TITLE = "JMA General Seasonal Forecasts (VPZK50)"
    DESCRIPTION = (
        "全般１か月予報・３か月予報・暖寒候期予報 — General Monthly/Seasonal Forecast. "
        "JMA extended forecasts covering 1-month, 3-month, and warm/cold season outlooks "
        "for Japan, including temperature and precipitation anomaly predictions.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, title_en, "
        "forecast_issued_time, forecast_type_en, forecast_period\n\n"
        "**Feed:** regular_l.xml | **Type code:** VPZK50\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** seasonal climate outlooks, agricultural planning, "
        "energy demand forecasting, climate anomaly research"
    )
    SUBTITLE = "1-month, 3-month, and warm/cold season temperature and precipitation forecasts"
    KEYWORDS = ["jma", "japan", "forecast", "seasonal", "monthly", "temperature", "precipitation"]
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
    TITLE = "JMA Regional Seasonal Forecasts (VPCK50)"
    DESCRIPTION = (
        "地方１か月予報・３か月予報・暖寒候期予報 — Regional Monthly/Seasonal Forecast. "
        "JMA district-level extended forecasts providing 1-month, 3-month, and seasonal "
        "temperature and precipitation anomaly predictions by region across Japan.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, title_en, "
        "forecast_issued_time, forecast_type_en, forecast_period, region_name_en\n\n"
        "**Feed:** regular_l.xml | **Type code:** VPCK50\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** regional climate outlooks, prefectural planning, "
        "agricultural decisions, climate trend analysis"
    )
    SUBTITLE = "Regional 1-month, 3-month, and seasonal forecasts by district"
    KEYWORDS = ["jma", "japan", "regional", "forecast", "seasonal", "district", "temperature"]
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
