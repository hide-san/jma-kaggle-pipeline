"""
Regional sea warning and forecast data from JMA.

Includes two official resources:
1. 地方海上警報 (Regional Sea Alert) - Data Type Code: VPCU51
2. 地方海上予報 (Regional Sea Forecast) - Data Type Code: VPCY51

Source Feed: other_l.xml
"""

import xml.etree.ElementTree as ET

import pandas as pd

from .base import JMADatasetBase, register_dataset

__all__ = ["RegionalSeaAlert", "RegionalSeaForecast"]


@register_dataset
class RegionalSeaAlert(JMADatasetBase):
    """Regional sea alert warnings from JMA."""

    NAME = "japan-regional-sea-alert"
    CSV_FILENAME = "japan_regional_sea_alert.csv"
    FEED_NAME = "other_l.xml"
    TYPE_CODES = ("VPCU51",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Regional Sea Alert Warnings (VPCU51)"
    DESCRIPTION = (
        "地方海上警報 — Regional Sea Alert Warning. "
        "JMA maritime hazard warnings for designated coastal sea regions, covering "
        "strong winds, high waves, fog, and storm surge affecting coastal navigation.\n\n"
        "**Columns include:** event_id, report_datetime, warning_type_en, "
        "region_name_en, region_code\n\n"
        "**Feed:** other_l.xml | **Type code:** VPCU51\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 250\n"
        "**Use cases:** coastal maritime safety, fishing vessel alerts, "
        "storm surge monitoring, port operations"
    )
    SUBTITLE = "Regional maritime hazard alerts with warning types and affected coastal regions"
    KEYWORDS = ["jma", "japan", "sea", "alert", "maritime", "tsunami", "high-wave", "coastal"]
    MAX_ENTRIES = 250

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VPCU51 regional sea alert XML."""
        return self._parse_sea_data(root, 'VPCU51')

    def _parse_sea_data(self, root: ET.Element, data_type: str) -> dict | None:
        """Parse JMA VPCU51/VPCY51 sea warning/forecast XML."""
        # Extract header info
        head_data = self.extract_head(root)
        if not head_data.get('report_datetime'):
            return None

        # Find body
        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/meteorology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        row = {
            'event_id': f"{head_data.get('report_datetime')}_{data_type}",
            'report_datetime': head_data.get('report_datetime'),
        }

        # Extract warning/forecast type and affected region
        warning_type_key = 'warning_type' if data_type == 'VPCU51' else 'forecast_type'
        warning_type_en_key = warning_type_key + '_en'

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'Warning' or tag == 'Forecast':
                # Extract warning/forecast info from nested Item
                for item_elem in elem:
                    if self.sn(item_elem.tag) == 'Item':
                        for item_child in item_elem:
                            item_child_tag = self.sn(item_child.tag)
                            if item_child_tag == 'Kind':
                                for kind_child in item_child:
                                    if self.sn(kind_child.tag) == 'Name' and kind_child.text:
                                        row[warning_type_key] = kind_child.text
                                        row[warning_type_en_key] = self.translate(kind_child.text)
                            elif item_child_tag == 'Area':
                                for area_child in item_child:
                                    area_child_tag = self.sn(area_child.tag)
                                    if area_child_tag == 'Name' and area_child.text:
                                        row['region_name'] = area_child.text
                                        row['region_name_en'] = self.translate(area_child.text)
                                    elif area_child_tag == 'Code' and area_child.text:
                                        row['region_code'] = area_child.text

        return row if len(row) > 2 else None


@register_dataset
class RegionalSeaForecast(JMADatasetBase):
    """Regional sea forecast data from JMA."""

    NAME = "japan-regional-sea-forecast"
    CSV_FILENAME = "japan_regional_sea_forecast.csv"
    FEED_NAME = "other_l.xml"
    TYPE_CODES = ("VPCY51",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Regional Sea Forecasts (VPCY51)"
    DESCRIPTION = (
        "地方海上予報 — Regional Sea Forecast. "
        "JMA maritime condition forecasts for designated coastal sea regions, predicting "
        "wave heights, wind speed and direction, and swell for safe navigation planning.\n\n"
        "**Columns include:** event_id, report_datetime, forecast_type_en, "
        "region_name_en, region_code\n\n"
        "**Feed:** other_l.xml | **Type code:** VPCY51\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 250\n"
        "**Use cases:** maritime route planning, fishing operations, "
        "coastal engineering, ocean weather research"
    )
    SUBTITLE = "Maritime forecasts with predicted wave heights, wind, and conditions by region"
    KEYWORDS = ["jma", "japan", "sea", "forecast", "maritime", "wave-height", "wind", "coastal"]
    MAX_ENTRIES = 250

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VPCY51 regional sea forecast XML."""
        # Extract header info
        head_data = self.extract_head(root)
        if not head_data.get('report_datetime'):
            return None

        # Find body
        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/meteorology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        row = {
            'event_id': f"{head_data.get('report_datetime')}_VPCY51",
            'report_datetime': head_data.get('report_datetime'),
        }

        # Extract forecast type and affected region
        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'Forecast':
                for item_elem in elem:
                    if self.sn(item_elem.tag) == 'Item':
                        for item_child in item_elem:
                            item_child_tag = self.sn(item_child.tag)
                            if item_child_tag == 'Kind':
                                for kind_child in item_child:
                                    if self.sn(kind_child.tag) == 'Name' and kind_child.text:
                                        row['forecast_type'] = kind_child.text
                                        row['forecast_type_en'] = self.translate(kind_child.text)
                            elif item_child_tag == 'Area':
                                for area_child in item_child:
                                    area_child_tag = self.sn(area_child.tag)
                                    if area_child_tag == 'Name' and area_child.text:
                                        row['region_name'] = area_child.text
                                        row['region_name_en'] = self.translate(area_child.text)
                                    elif area_child_tag == 'Code' and area_child.text:
                                        row['region_code'] = area_child.text

        return row if len(row) > 2 else None


# For backwards compatibility
def fetch_sea_warnings() -> pd.DataFrame:
    """Legacy function wrapper."""
    return RegionalSeaAlert().fetch()


def fetch_sea_forecasts() -> pd.DataFrame:
    """Legacy function wrapper."""
    return RegionalSeaForecast().fetch()
