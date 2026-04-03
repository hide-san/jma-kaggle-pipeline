"""
Cherry blossom phenological observation data from JMA.

Official Resource: 生物季節観測 (Phenological Observation)
Data Type Code: VGSK55
Source Feed: other_l.xml
"""

import xml.etree.ElementTree as ET

import pandas as pd

from .base import JMADatasetBase, register_dataset

__all__ = ["PhenologicalObservation"]


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
    DESCRIPTION = "Phenological observations including cherry blossom blooming stages from JMA"
    SUBTITLE = "Annual cherry blossom and seasonal phenological data"
    KEYWORDS = ["jma", "japan", "cherry-blossom", "phenology", "observation"]
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


# For backwards compatibility: export the fetch function
def fetch_cherry_blossom_observations() -> pd.DataFrame:
    """Legacy function wrapper."""
    return PhenologicalObservation().fetch()
