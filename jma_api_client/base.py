"""
Base class and registry for JMA dataset implementations.

Plugin architecture: Each dataset class inherits JMADatasetBase and is
decorated with @register_dataset. The registry automatically collects all
implementations for use in config.py and data_pipeline.py.
"""

import os
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from typing import ClassVar, Generator

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed, Retrying

from logger import get_logger
from .translate import translate_ja_to_en
from .utils import get as http_get

import config

log = get_logger(__name__)

# Global registry: dataset_name -> dataset class
DATASET_REGISTRY: dict[str, type["JMADatasetBase"]] = {}


def register_dataset(cls: type["JMADatasetBase"]) -> type["JMADatasetBase"]:
    """Class decorator to register a dataset in DATASET_REGISTRY."""
    DATASET_REGISTRY[cls.NAME] = cls
    return cls


# ========== Helper Functions ==========

def strip_ns(tag: str) -> str:
    """Strip XML namespace from tag. '{http://...}Tag' -> 'Tag'"""
    return tag.split('}')[-1] if '}' in tag else tag


def find_text(elem: ET.Element, *tag_names: str) -> str | None:
    """Find first matching descendant element's text."""
    for tag in tag_names:
        for child in elem.iter():
            if strip_ns(child.tag) == tag and child.text:
                return child.text
    return None


def find_all_text(elem: ET.Element, tag_name: str) -> list[str]:
    """Find all matching descendant elements' texts."""
    return [child.text for child in elem.iter()
            if strip_ns(child.tag) == tag_name and child.text]


JMA_FEED_URLS: dict[str, str] = {
    "regular_l.xml": "https://www.data.jma.go.jp/developer/xml/feed/regular_l.xml",
    "extra_l.xml":   "https://www.data.jma.go.jp/developer/xml/feed/extra_l.xml",
    "eqvol_l.xml":   "https://www.data.jma.go.jp/developer/xml/feed/eqvol_l.xml",
    "other_l.xml":   "https://www.data.jma.go.jp/developer/xml/feed/other_l.xml",
}


def fetch_all_feeds() -> None:
    """Download all JMA Atom feeds and cache them in RAW_DATA_DIR.

    Must be called once at the start of the pipeline before any dataset
    fetch() call, since get_feed() reads from the local cache.
    Raises RuntimeError if any feed fails to download.
    """
    from .utils import get as http_get, save_raw
    os.makedirs(config.RAW_DATA_DIR, exist_ok=True)
    failed = []
    for feed_name, feed_url in JMA_FEED_URLS.items():
        try:
            log.info("Fetching feed %s", feed_name)
            resp = http_get(feed_url)
            save_raw(feed_name, resp.content)
        except Exception as exc:
            log.error("Could not fetch feed %s: %s", feed_name, exc)
            failed.append(feed_name)
    if failed:
        raise RuntimeError("Failed to fetch feeds: %s" % ", ".join(failed))


def get_feed(feed_name: str) -> ET.Element | None:
    """Load cached feed from RAW_DATA_DIR."""
    import config
    feed_path = os.path.join(config.RAW_DATA_DIR, feed_name)

    if not os.path.exists(feed_path):
        log.warning("Feed %s not found in %s", feed_name, config.RAW_DATA_DIR)
        return None

    try:
        with open(feed_path, 'rb') as f:
            content = f.read()
        return ET.fromstring(content)
    except Exception as e:
        log.error("Could not parse %s: %s", feed_name, e)
        return None


def iter_feed_entries(feed_name: str, *type_codes: str) -> Generator:
    """
    Iterate through Atom feed entries, filtering by type code prefixes.
    Yields (data_url, entry_element).
    """
    root = get_feed(feed_name)
    if root is None:
        return

    ns = {'atom': 'http://www.w3.org/2005/Atom'}
    entries = root.findall('.//atom:entry', ns)

    for entry in entries:
        link = entry.find('atom:link', ns)
        if link is None:
            continue

        data_url = link.get('href')
        if not data_url:
            continue

        # Check if URL contains any of the type codes (prefix match)
        if any(code in data_url for code in type_codes):
            yield data_url, entry


# ========== Base Class ==========

class JMADatasetBase(ABC):
    """
    Abstract base class for JMA dataset implementations.

    Each subclass must:
    1. Set class variables: NAME, CSV_FILENAME, FEED_NAME, TYPE_CODES, MERGE_KEYS
    2. Implement parse_entry() to parse individual XML entries
    3. Use @register_dataset decorator
    """

    NAME: ClassVar[str]                      # dataset slug (e.g., 'japan-earthquake-intensity-info')
    CSV_FILENAME: ClassVar[str]              # output CSV filename (e.g., 'japan_earthquake_intensity_info.csv')
    FEED_NAME: ClassVar[str]                 # 'eqvol_l.xml' | 'other_l.xml' | 'regular_l.xml' | 'extra_l.xml'
    TYPE_CODES: ClassVar[tuple[str, ...]]    # Data type code prefixes (e.g., ('VXSE53',))
    MERGE_KEYS: ClassVar[list[str]]          # Columns to deduplicate on (e.g., ['event_id'])
    TITLE: ClassVar[str] = ""               # Human-readable dataset title shown on Kaggle
    DESCRIPTION: ClassVar[str] = ""
    SUBTITLE: ClassVar[str] = ""
    KEYWORDS: ClassVar[list[str]] = []
    MAX_ENTRIES: ClassVar[int] = 100

    def __init__(self):
        self.log = log

    @abstractmethod
    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """
        Parse a single JMA XML entry and return a row dictionary.
        Return None to skip this entry.

        Args:
            root: Root element of the detailed XML file
            data_url: URL of the data file (for reference/logging)

        Returns:
            Dictionary with all fields, or None to skip
        """
        pass

    @retry(
        stop=stop_after_attempt(config.RETRY_ATTEMPTS),
        wait=wait_fixed(config.RETRY_WAIT_SECONDS)
    )
    def fetch(self) -> pd.DataFrame:
        """
        Main fetch method: iterate feed entries → parse → return DataFrame.
        """
        rows = []
        entry_count = 0

        for data_url, entry in iter_feed_entries(self.FEED_NAME, *self.TYPE_CODES):
            try:
                # Fetch with per-entry retry to recover from transient JMA errors
                for attempt in Retrying(stop=stop_after_attempt(3), wait=wait_fixed(2), reraise=True):
                    with attempt:
                        resp = http_get(data_url)

                data_root = ET.fromstring(resp.content)
                row = self.parse_entry(data_root, data_url)
                if row:
                    rows.append(row)
                    entry_count += 1

                if entry_count >= self.MAX_ENTRIES:
                    self.log.warning(
                        "%s: reached MAX_ENTRIES limit (%d), stopping early",
                        self.NAME, self.MAX_ENTRIES,
                    )
                    break

            except Exception as e:
                self.log.debug("Failed to parse %s: %s", data_url, e)
                continue

        df = pd.DataFrame(rows)
        self.log.info("%s: %d rows fetched", self.NAME, len(df))
        return df

    def extract_head(self, root: ET.Element) -> dict:
        """
        Extract common header info from JMA XML Head element.

        Returns:
            Dict with keys: event_id, report_datetime, info_type, title
        """
        head = root.find('.//{http://xml.kishou.go.jp/jmaxml1/informationBasis1/}Head')
        if head is None:
            head = root.find('.//{http://xml.kishou.go.jp/jmaxml1/}Head')

        if head is None:
            return {}

        data = {}
        for elem in head.iter():
            tag = strip_ns(elem.tag)

            if tag == 'EventID' and elem.text:
                data['event_id'] = elem.text
            elif tag == 'ReportDateTime' and elem.text:
                data['report_datetime'] = elem.text
            elif tag == 'InfoType' and elem.text:
                data['info_type'] = elem.text
                data['info_type_en'] = translate_ja_to_en(elem.text)
            elif tag == 'Title' and elem.text:
                data['title'] = elem.text
                data['title_en'] = translate_ja_to_en(elem.text)

        return data

    def translate(self, text: str) -> str:
        """Translate Japanese text to English (cached)."""
        if not text:
            return ""
        return translate_ja_to_en(text)

    def sn(self, tag: str) -> str:
        """Strip namespace from tag."""
        return strip_ns(tag)

    def find_text(self, elem: ET.Element, *tags: str) -> str | None:
        """Find first matching child/descendant text."""
        return find_text(elem, *tags)

    def find_all_text(self, elem: ET.Element, tag: str) -> list[str]:
        """Find all matching child/descendant texts."""
        return find_all_text(elem, tag)

    @classmethod
    def to_config(cls) -> dict:
        """Generate config dict for DATASETS list."""
        return {
            "name": cls.NAME,
            "kaggle_dataset": f"{{KAGGLE_USERNAME}}/{cls.NAME}",
            "csv_filename": cls.CSV_FILENAME,
            "merge_keys": cls.MERGE_KEYS,
            "title": cls.TITLE,
            "description": cls.DESCRIPTION,
            "subtitle": cls.SUBTITLE,
            "keywords": cls.KEYWORDS,
        }

    def get_metadata(self) -> dict:
        """Return metadata about this dataset for observability."""
        return {
            "name": self.NAME,
            "feed": self.FEED_NAME,
            "type_codes": self.TYPE_CODES,
            "max_entries": self.MAX_ENTRIES,
            "merge_keys": self.MERGE_KEYS,
            "description": self.DESCRIPTION,
        }
