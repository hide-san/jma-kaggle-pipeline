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
    """
    Cherry blossom and other phenological observations from JMA.

    Extracts observation date, phenophase (e.g., "full bloom"), and station info.
    """

    NAME = "japan-phenological-observation"
    CSV_FILENAME = "japan_phenological_observation.csv"
    FEED_NAME = "other_l.xml"
    TYPE_CODES = ("VGSK55",)
    MERGE_KEYS = ["event_id"]
    DESCRIPTION = "Phenological observations (VGSK55) tracking cherry blossom blooming phenophases from JMA monitoring stations nationwide. Includes observation dates, blooming stages, and station locations for seasonal research."
    SUBTITLE = "Cherry blossom and tree phenophase observations by monitoring station and date"
    KEYWORDS = ["jma", "japan", "cherry-blossom", "phenology", "observation", "bloom", "seasonal"]
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

    NAME = "japan-seasonal-observation"
    CSV_FILENAME = "japan_seasonal_observation.csv"
    FEED_NAME = "other_l.xml"
    TYPE_CODES = ("VGSK50",)
    MERGE_KEYS = ["event_id"]
    DESCRIPTION = "Seasonal observations (VGSK50) of various tree and plant phenophases from nationwide JMA monitoring stations. Records observation dates, phenophase types, and station information for seasonal ecology research."
    SUBTITLE = "Seasonal tree and plant phenophase observations with station data"
    KEYWORDS = ["jma", "japan", "seasonal", "observation", "phenology", "plants", "trees"]
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

    NAME = "japan-special-weather-report"
    CSV_FILENAME = "japan_special_weather_report.csv"
    FEED_NAME = "other_l.xml"
    TYPE_CODES = ("VGSK60",)
    MERGE_KEYS = ["event_id"]
    DESCRIPTION = "Special weather reports (VGSK60) documenting unusual or notable weather phenomena observed nationwide. Includes phenomenon type, location, and observation details for meteorological documentation."
    SUBTITLE = "Notable weather phenomena reports with locations and descriptions"
    KEYWORDS = ["jma", "japan", "weather", "special-report", "phenomenon", "unusual"]
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
