"""
Extended weather forecasts and seasonal predictions from JMA.

Official Resources:
1. 全般１か月予報・３か月予報・暖寒候期予報 (General Monthly/Seasonal Forecasts) - VPZK50
2. 地方１か月予報・３か月予報・暖寒候期予報 (Regional Monthly/Seasonal Forecasts) - VPCK50

Source Feed: regular_l.xml
"""

import xml.etree.ElementTree as ET

from .base import JMADatasetBase, register_dataset

__all__ = [
    "GeneralSeasonalForecast",
    "RegionalSeasonalForecast",
]


@register_dataset
class GeneralSeasonalForecast(JMADatasetBase):
    """General monthly and seasonal forecasts from JMA VPZK50."""

    NAME = "jma-general-seasonal-forecast"
    CSV_FILENAME = "jma_general_seasonal_forecast.csv"
    FEED_NAME = "regular_l.xml"
    TYPE_CODES = ("VPZK50",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA General Seasonal Forecast"
    DESCRIPTION = (
        "全般１か月予報・３か月予報・暖寒候期予報 — General Monthly/Seasonal Forecast. "
        "JMA extended forecasts covering 1-month, 3-month, and warm/cold season outlooks "
        "for Japan, including temperature and precipitation anomaly predictions.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, title_en, "
        "target_area_name_en, target_area_code, forecast_start_date, forecast_duration, "
        "forecast_period_name_en, next_forecast_datetime\n\n"
        "**Feed:** regular_l.xml | **Type code:** VPZK50\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** seasonal climate outlooks, agricultural planning, "
        "energy demand forecasting, climate anomaly research"
    )
    SUBTITLE = "1-month, 3-month, and warm/cold season temperature and precipitation forecasts"
    KEYWORDS = ["japan", "weather", "temperature", "climate-change", "asia"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VPZK50 general seasonal forecast XML."""
        head_data = self.extract_head(root)

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/meteorology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        # Seasonal forecasts have empty EventID — synthesize from report_datetime + title
        if not head_data.get('event_id') and head_data.get('report_datetime'):
            head_data['event_id'] = f"{head_data['report_datetime']}_{head_data.get('title', 'VPZK50')}"

        if not head_data.get('event_id'):
            return None

        forecast_data = head_data.copy()

        # Extract TargetArea (national or regional scope)
        for elem in body:
            tag = self.sn(elem.tag)
            if tag == 'TargetArea':
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Name' and child.text:
                        forecast_data['target_area_name'] = child.text
                        forecast_data['target_area_name_en'] = self.translate(child.text)
                    elif child_tag == 'Code' and child.text:
                        forecast_data['target_area_code'] = child.text
                break

        # Extract forecast period metadata from first MeteorologicalInfo
        for elem in body.iter():
            if self.sn(elem.tag) == 'MeteorologicalInfo':
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'DateTime' and child.text:
                        forecast_data['forecast_start_date'] = child.text
                    elif child_tag == 'Duration' and child.text:
                        forecast_data['forecast_duration'] = child.text
                    elif child_tag == 'Name' and child.text:
                        forecast_data['forecast_period_name'] = child.text
                        forecast_data['forecast_period_name_en'] = self.translate(child.text)
                break  # only the first MeteorologicalInfo block

        # Extract next forecast schedule from AdditionalInfo
        for elem in body.iter():
            if self.sn(elem.tag) == 'NextForecastSchedule':
                for child in elem:
                    if self.sn(child.tag) == 'DateTime' and child.text:
                        forecast_data['next_forecast_datetime'] = child.text
                break

        return forecast_data if len(forecast_data) > 2 else None


@register_dataset
class RegionalSeasonalForecast(JMADatasetBase):
    """Regional monthly and seasonal forecasts from JMA VPCK50."""

    NAME = "jma-regional-seasonal-forecast"
    CSV_FILENAME = "jma_regional_seasonal_forecast.csv"
    FEED_NAME = "regular_l.xml"
    TYPE_CODES = ("VPCK50",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Regional Seasonal Forecast"
    DESCRIPTION = (
        "地方１か月予報・３か月予報・暖寒候期予報 — Regional Monthly/Seasonal Forecast. "
        "JMA district-level extended forecasts providing 1-month, 3-month, and seasonal "
        "temperature and precipitation anomaly predictions by region across Japan.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, title_en, "
        "target_area_name_en, target_area_code, forecast_start_date, forecast_duration, "
        "forecast_period_name_en, next_forecast_datetime\n\n"
        "**Feed:** regular_l.xml | **Type code:** VPCK50\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** regional climate outlooks, prefectural planning, "
        "agricultural decisions, climate trend analysis"
    )
    SUBTITLE = "Regional 1-month, 3-month, and seasonal forecasts by district"
    KEYWORDS = ["japan", "weather", "temperature", "climate-change", "east-asia"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VPCK50 regional seasonal forecast XML."""
        head_data = self.extract_head(root)

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/meteorology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        # Seasonal forecasts have empty EventID — synthesize from report_datetime + title
        if not head_data.get('event_id') and head_data.get('report_datetime'):
            head_data['event_id'] = f"{head_data['report_datetime']}_{head_data.get('title', 'VPCK50')}"

        if not head_data.get('event_id'):
            return None

        forecast_data = head_data.copy()

        # Extract TargetArea (regional scope)
        for elem in body:
            tag = self.sn(elem.tag)
            if tag == 'TargetArea':
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Name' and child.text:
                        forecast_data['target_area_name'] = child.text
                        forecast_data['target_area_name_en'] = self.translate(child.text)
                    elif child_tag == 'Code' and child.text:
                        forecast_data['target_area_code'] = child.text
                break

        # Extract forecast period metadata from first MeteorologicalInfo
        for elem in body.iter():
            if self.sn(elem.tag) == 'MeteorologicalInfo':
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'DateTime' and child.text:
                        forecast_data['forecast_start_date'] = child.text
                    elif child_tag == 'Duration' and child.text:
                        forecast_data['forecast_duration'] = child.text
                    elif child_tag == 'Name' and child.text:
                        forecast_data['forecast_period_name'] = child.text
                        forecast_data['forecast_period_name_en'] = self.translate(child.text)
                break  # only the first MeteorologicalInfo block

        # Extract next forecast schedule from AdditionalInfo
        for elem in body.iter():
            if self.sn(elem.tag) == 'NextForecastSchedule':
                for child in elem:
                    if self.sn(child.tag) == 'DateTime' and child.text:
                        forecast_data['next_forecast_datetime'] = child.text
                break

        return forecast_data if len(forecast_data) > 2 else None
