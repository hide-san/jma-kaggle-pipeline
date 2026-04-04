"""
Phenological and seasonal observation data from JMA.

Official Resources:
1. 生物季節観測 (Phenological Observation) - VGSK55
2. 季節観測 (Seasonal Observation) - VGSK50
3. 特殊気象報 (Special Weather Report) - VGSK60

Source Feed: other_l.xml
"""

import xml.etree.ElementTree as ET

import pandas as pd

from .base import JMADatasetBase, register_dataset

__all__ = [
    "PhenologicalObservation",
    "SeasonalObservation",
    "SpecialWeatherReport",
]


@register_dataset
class PhenologicalObservation(JMADatasetBase):
    """Phenological observations (plants, insects, birds) from JMA VGSK55."""

    NAME = "jma-phenological-observations"
    CSV_FILENAME = "jma_phenological_observations.csv"
    FEED_NAME = "other_l.xml"
    TYPE_CODES = ("VGSK55",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Phenological Observations"
    DESCRIPTION = (
        "生物季節観測 — Phenological Observation. "
        "JMA observations of biological phenophases from monitoring stations nationwide, "
        "covering plants (cherry blossom, plum, maple), insects (first firefly, cicada), "
        "and birds (first cuckoo, swallow), tracking first and full phenophase dates.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, title_en, "
        "observation_date, phenophase_en, phenophase_code, "
        "station_name_en, station_location_en\n\n"
        "**Feed:** other_l.xml | **Type code:** VGSK55\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** phenology research, climate change impact studies, "
        "seasonal pattern analysis, biodiversity monitoring"
    )
    SUBTITLE = "Biological phenophase observations (plants, insects, birds) by station and date"
    KEYWORDS = ["japan", "plants", "environment", "climate-change", "asia"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VGSK55 phenological observation XML."""
        # Extract header info (event_id, report_datetime)
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        # Find body
        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/meteorology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        row = head_data.copy()

        # Extract observation date, phenophase, and station info from body
        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'DateTime' and elem.text:
                row['observation_date'] = elem.text

            elif tag == 'Kind':
                # Phenophase (e.g., "桜の花が咲いている")
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Name' and child.text:
                        row['phenophase'] = child.text
                        row['phenophase_en'] = self.translate(child.text)
                    elif child_tag == 'Code' and child.text:
                        row['phenophase_code'] = child.text

            elif tag == 'Station':
                # Station name and location
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Name' and child.text:
                        row['station_name'] = child.text
                        row['station_name_en'] = self.translate(child.text)
                    elif child_tag == 'Location' and child.text:
                        row['station_location'] = child.text
                        row['station_location_en'] = self.translate(child.text)

        # Return only if we extracted meaningful data
        return row if len(row) > 2 else None


@register_dataset
class SeasonalObservation(JMADatasetBase):
    """General seasonal observations from JMA VGSK50."""

    NAME = "jma-seasonal-observation"
    CSV_FILENAME = "jma_seasonal_observation.csv"
    FEED_NAME = "other_l.xml"
    TYPE_CODES = ("VGSK50",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Seasonal Observation"
    DESCRIPTION = (
        "季節観測 — Seasonal Observation. "
        "JMA nationwide observations of seasonal natural phenomena including "
        "first frost, ice, and other weather-related seasonal indicators by station.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, title_en, "
        "observation_date, observation_type_en, observation_code, "
        "station_name_en, station_location_en\n\n"
        "**Feed:** other_l.xml | **Type code:** VGSK50\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** seasonal climate monitoring, first-frost/ice records, "
        "ecological research, long-term climate trend analysis"
    )
    SUBTITLE = "Seasonal tree and plant phenophase observations with station data"
    KEYWORDS = ["japan", "plants", "environment", "climate-change", "asia"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VGSK50 seasonal observation XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/meteorology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        row = head_data.copy()

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'DateTime' and elem.text:
                row['observation_date'] = elem.text

            elif tag == 'Kind':
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Name' and child.text:
                        row['observation_type'] = child.text
                        row['observation_type_en'] = self.translate(child.text)
                    elif child_tag == 'Code' and child.text:
                        row['observation_code'] = child.text

            elif tag == 'Station':
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Name' and child.text:
                        row['station_name'] = child.text
                        row['station_name_en'] = self.translate(child.text)
                    elif child_tag == 'Location' and child.text:
                        row['station_location'] = child.text
                        row['station_location_en'] = self.translate(child.text)

        return row if len(row) > 2 else None


@register_dataset
class SpecialWeatherReport(JMADatasetBase):
    """Special weather reports from JMA VGSK60."""

    NAME = "jma-special-weather-report"
    CSV_FILENAME = "jma_special_weather_report.csv"
    FEED_NAME = "other_l.xml"
    TYPE_CODES = ("VGSK60",)
    MERGE_KEYS = ["event_id"]
    TITLE = "JMA Special Weather Report"
    DESCRIPTION = (
        "特殊気象報 — Special Weather Report. "
        "JMA documentation of unusual or historically significant weather phenomena "
        "observed at monitoring stations across Japan, with location and phenomenon details.\n\n"
        "**Columns include:** event_id, report_datetime, info_type_en, title_en, "
        "report_date, weather_phenomenon_en, area_name_en\n\n"
        "**Feed:** other_l.xml | **Type code:** VGSK60\n"
        "**Updates:** Hourly automated pipeline | **Max entries per run:** 100\n"
        "**Use cases:** extreme weather documentation, climate records, "
        "meteorological history research, rare event cataloging"
    )
    SUBTITLE = "Notable weather phenomena reports with locations and descriptions"
    KEYWORDS = ["japan", "weather", "events", "government", "east-asia"]
    MAX_ENTRIES = 100

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Parse JMA VGSK60 special weather report XML."""
        head_data = self.extract_head(root)
        if not head_data.get('event_id'):
            return None

        body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/body/meteorology1/}Body')
        if body is None:
            body = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Body')

        if body is None:
            return None

        row = head_data.copy()

        for elem in body.iter():
            tag = self.sn(elem.tag)

            if tag == 'DateTime' and elem.text:
                row['report_date'] = elem.text

            elif tag == 'Item':
                for child in elem:
                    child_tag = self.sn(child.tag)
                    if child_tag == 'Kind':
                        for kind_child in child:
                            if self.sn(kind_child.tag) == 'Name' and kind_child.text:
                                row['weather_phenomenon'] = kind_child.text
                                row['weather_phenomenon_en'] = self.translate(kind_child.text)
                    elif child_tag == 'Area':
                        for area_child in child:
                            if self.sn(area_child.tag) == 'Name' and area_child.text:
                                row['area_name'] = area_child.text
                                row['area_name_en'] = self.translate(area_child.text)

        return row if len(row) > 2 else None


# For backwards compatibility: export the fetch function
def fetch_cherry_blossom_observations() -> pd.DataFrame:
    """Legacy function wrapper."""
    return PhenologicalObservation().fetch()
