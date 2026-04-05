"""
Weather alert and forecast data from JMA.

Official Resources:
1. 気象警報・注意報（定時） (Weather Warnings/Advisories) - VPWW53-61
2. 記録的短時間大雨情報 (Record Short-term Heavy Rain) - VPOA50
3. 竜巻注意情報 (Tornado Watch Information) - VPHW50/51

Source Feed: regular_l.xml (weather alerts and warnings)
"""

import xml.etree.ElementTree as ET

from .base import JMADatasetBase, register_dataset

__all__ = [
    "WeatherWarning",
    "HeavyRainWarning",
    "TornadoWatchInfo",
]


@register_dataset
class WeatherWarning(JMADatasetBase):
    """General weather warnings and advisories from JMA VPWW53."""

    NAME = "jma-weather-warning-and-advisory"
    CSV_FILENAME = "jma_weather_warning_and_advisory.csv"
    FEED_NAME = "regular_l.xml"
    TYPE_CODES = ("VPWW53", "VPWW54", "VPWW55", "VPWW56", "VPWW57", "VPWW58", "VPWW59", "VPWW60", "VPWW61")
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Weather Warning and Advisory"
    DESCRIPTION = (
        "気象警報・注意報 — Weather Warnings & Advisories. "
        "JMA severe weather alerts covering heavy rain, snow, strong winds, high waves, "
        "flooding, and lightning issued nationwide with affected regions and warning severity.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, title_en, "
        "warning_type_en, region_name_en\n\n"
        "**Feed:** regular_l.xml | **Type codes:** VPWW53–VPWW61\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 500\n"
        "**Use cases:** severe weather monitoring, disaster preparedness, "
        "regional hazard analysis, climate risk research"
    )
    SUBTITLE = "Severe weather alerts (rain, snow, wind, waves, floods) by region and type"
    KEYWORDS = ["japan", "weather", "atmospheric-science", "public-safety", "east-asia"]
    MAX_ENTRIES = 500

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VPWW weather warning XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/meteorology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        warning_data = head_data.copy()

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'Warning':
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Item':
                        for item_child in child:
                            item_tag = self.sn(item_child.tag)
                            if item_tag == 'Kind':
                                for kind_child in item_child:
                                    if self.sn(kind_child.tag) == 'Name' and kind_child.text:
                                        warning_data['warning_type'] = kind_child.text
                                        warning_data['warning_type_en'] = self.translate(kind_child.text)
                            elif item_tag == 'Area':
                                for area_child in item_child:
                                    if self.sn(area_child.tag) == 'Name' and area_child.text:
                                        warning_data['region_name'] = area_child.text
                                        warning_data['region_name_en'] = self.translate(area_child.text)

        return warning_data if len(warning_data) > 2 else None


@register_dataset
class HeavyRainWarning(JMADatasetBase):
    """Record short-term heavy rain alerts from JMA VPOA50."""

    NAME = "jma-record-short-time-heavy-rain"
    CSV_FILENAME = "jma_record_short_time_heavy_rain.csv"
    FEED_NAME = "regular_l.xml"
    TYPE_CODES = ("VPOA50",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Record Short-time Heavy Rain Information"
    DESCRIPTION = (
        "記録的短時間大雨情報 — Record Short-term Heavy Rain Information. "
        "JMA alerts for exceptionally intense short-duration rainfall events that may "
        "trigger flash floods or landslides, issued with affected regions for emergency response.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, title_en, "
        "rainfall_type_en, region_name_en\n\n"
        "**Feed:** regular_l.xml | **Type code:** VPOA50\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** flash flood detection, landslide risk, extreme rainfall events, "
        "climate extremes research"
    )
    SUBTITLE = "Exceptional rainfall event alerts with affected regions and intensity"
    KEYWORDS = ["japan", "weather", "natural-disaster", "public-safety", "east-asia"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VPOA50 heavy rain warning XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/meteorology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        rain_data = head_data.copy()

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'Item':
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Kind':
                        for kind_child in child:
                            if self.sn(kind_child.tag) == 'Name' and kind_child.text:
                                rain_data['rainfall_type'] = kind_child.text
                                rain_data['rainfall_type_en'] = self.translate(kind_child.text)
                    elif child_tag == 'Area':
                        for area_child in child:
                            if self.sn(area_child.tag) == 'Name' and area_child.text:
                                rain_data['region_name'] = area_child.text
                                rain_data['region_name_en'] = self.translate(area_child.text)

        return rain_data if len(rain_data) > 2 else None


@register_dataset
class TornadoWatchInfo(JMADatasetBase):
    """Tornado watch information from JMA VPHW50/51."""

    NAME = "jma-tornado-watch-information"
    CSV_FILENAME = "jma_tornado_watch_information.csv"
    FEED_NAME = "regular_l.xml"
    TYPE_CODES = ("VPHW50", "VPHW51")
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Tornado Watch Information"
    DESCRIPTION = (
        "竜巻注意情報 — Tornado Watch Information. "
        "JMA alerts issued when atmospheric conditions become favorable for tornado development, "
        "specifying watch zones and time windows for severe weather preparedness.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, title_en, "
        "alert_type_en, region_name_en, region_code\n\n"
        "**Feed:** regular_l.xml | **Type codes:** VPHW50, VPHW51\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** severe weather monitoring, tornado risk research, "
        "emergency preparedness, atmospheric science"
    )
    SUBTITLE = "Tornado watch alerts with affected areas and atmospheric conditions"
    KEYWORDS = ["japan", "weather", "atmospheric-science", "public-safety", "alerts"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VPHW tornado watch XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/meteorology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        watch_data = head_data.copy()

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'Item':
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Kind':
                        for kind_child in child:
                            if self.sn(kind_child.tag) == 'Name' and kind_child.text:
                                watch_data['alert_type'] = kind_child.text
                                watch_data['alert_type_en'] = self.translate(kind_child.text)
                    elif child_tag == 'Area':
                        for area_child in child:
                            if self.sn(area_child.tag) == 'Name' and area_child.text:
                                watch_data['region_name'] = area_child.text
                                watch_data['region_name_en'] = self.translate(area_child.text)
                            elif self.sn(area_child.tag) == 'Code' and area_child.text:
                                watch_data['region_code'] = area_child.text

        return watch_data if len(watch_data) > 2 else None
