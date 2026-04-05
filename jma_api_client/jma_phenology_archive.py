"""
Historical phenological observations archive (累年値) from JMA.

Downloads the bulk ZIP from:
  https://www.data.jma.go.jp/sakura/data/ruinenchi/ruinenchi_all.zip

Each CSV in the ZIP covers one species.  The wide format (stations as rows,
years as columns) is reshaped into one row per (species, station, year).

Merge keys: [species_code, station_code, year]
"""

import io
import xml.etree.ElementTree as ET
import zipfile

import pandas as pd

from .base import JMADatasetBase, register_dataset
from .utils import get as http_get

ARCHIVE_URL = "https://www.data.jma.go.jp/sakura/data/ruinenchi/ruinenchi_all.zip"

# Missing-value markers used by JMA in these CSVs
_MISSING = {'', '/', '--', '×', '///'}


def _safe_int(cells: list[str], idx: int | None) -> int | None:
    if idx is None or idx >= len(cells):
        return None
    s = cells[idx].strip()
    if not s or s in _MISSING:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _mmdd_to_date(year: int, mmdd: int) -> str | None:
    """Convert a JMA MMDD integer (e.g. 322 → '1953-03-22') to an ISO date."""
    try:
        month = mmdd // 100
        day = mmdd % 100
        return f"{year:04d}-{month:02d}-{day:02d}"
    except Exception:
        return None


@register_dataset
class PhenologicalObservationArchive(JMADatasetBase):
    """
    Historical multi-year phenological archive (累年値) from JMA.

    Covers plants (cherry blossom, plum, ginkgo …) and animals (swallow,
    firefly, cicada …) observed at weather stations across Japan since 1953.
    """

    NAME = "jma-phenological-observations-archive"
    CSV_FILENAME = "jma_phenological_observations_archive.csv"
    FEED_NAME = "other_l.xml"   # not used — fetch() is fully overridden
    TYPE_CODES = ("VGSK55",)    # not used — fetch() is fully overridden
    MERGE_KEYS = ["species_code", "station_code", "year"]
    TITLE = "JMA Phenological Observations Archive"
    DESCRIPTION = (
        "生物季節観測累年値 — Multi-year phenological observation archive from the Japan "
        "Meteorological Agency (JMA) covering 70+ years of biological phenophase records "
        "(1953 to present) at weather stations across Japan.\n\n"
        "Includes flowering and leaf-change dates for plants (cherry blossom, plum, "
        "ginkgo, maple …) and first-sighting dates for animals (swallow, firefly, "
        "cicada, frog …) per station and year.\n\n"
        "**Columns include:** species_code, species_name_en, station_code, station_name_en, "
        "year, observation_mmdd, observation_date, remark, normal_value_mmdd, "
        "latest_date_mmdd, latest_date_year, earliest_date_mmdd, earliest_date_year\n\n"
        "**Source:** https://www.data.jma.go.jp/sakura/data/download_ruinenchi.html\n"
        "**Updates:** Hourly automated pipeline | **Merge keys:** species_code, station_code, year\n"
        "**Use cases:** climate change impact studies, phenology research, "
        "long-term seasonal trend analysis, biodiversity monitoring"
    )
    SUBTITLE = "70+ years of plant and animal phenophase dates by station (1953–present)"
    KEYWORDS = ["japan", "plants", "environment", "climate-change", "asia"]
    MAX_ENTRIES = 999999  # not used

    # ------------------------------------------------------------------
    # Main entry point (overrides base feed-based fetch)
    # ------------------------------------------------------------------

    def fetch(self) -> pd.DataFrame:
        """Download the archive ZIP and return a long-format DataFrame."""
        self.log.info("%s: downloading archive from %s", self.NAME, ARCHIVE_URL)
        resp = http_get(ARCHIVE_URL)

        rows: list[dict] = []
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            for name in sorted(zf.namelist()):
                if not name.lower().endswith('.csv'):
                    continue
                with zf.open(name) as f:
                    content = f.read().decode('shift-jis', errors='replace')
                parsed = self._parse_archive_csv(content)
                rows.extend(parsed)
                self.log.debug("%s: %d rows from %s", self.NAME, len(parsed), name)

        df = pd.DataFrame(rows)
        self.log.info("%s: %d total rows parsed from archive", self.NAME, len(df))
        return df

    # ------------------------------------------------------------------
    # Per-CSV parser
    # ------------------------------------------------------------------

    def _parse_archive_csv(self, content: str) -> list[dict]:
        """
        Parse one species CSV (wide format) into long-format row dicts.

        CSV structure
        -------------
        Line 0 : species_code, species_name, <empty cols…>
        Line 1 : 番号, 地点名, 1953, rm, 1954, rm, …, YYYY, rm, 平年値, rm,
                 最大値, rm, 最大年, 最小値, rm, 最小年
        Line 2+: station rows
        """
        lines = [ln for ln in content.splitlines() if ln.strip()]
        if len(lines) < 3:
            return []

        # --- species metadata (line 0) ---
        row0 = lines[0].split(',')
        species_code = row0[0].strip()
        species_name = row0[1].strip() if len(row0) > 1 else ''
        species_name_en = self.translate(species_name)

        # --- column header map (line 1) ---
        headers = lines[1].split(',')

        year_col: dict[int, int] = {}    # year → value column index
        remark_col: dict[int, int] = {}  # year → remark column index

        for i, h in enumerate(headers):
            h = h.strip()
            if h.isdigit() and len(h) == 4:
                yr = int(h)
                year_col[yr] = i
                if i + 1 < len(headers):
                    remark_col[yr] = i + 1

        # locate statistics columns
        normal_ci = next((i for i, h in enumerate(headers) if '平年値' in h), None)
        max_val_ci = next((i for i, h in enumerate(headers) if '最大値' in h), None)
        max_yr_ci = next((i for i, h in enumerate(headers) if '最大年' in h), None)
        min_val_ci = next((i for i, h in enumerate(headers) if '最小値' in h), None)
        min_yr_ci = next((i for i, h in enumerate(headers) if '最小年' in h), None)

        current_year = pd.Timestamp.now().year
        rows: list[dict] = []

        for line in lines[2:]:
            cells = line.split(',')
            if len(cells) < 2:
                continue
            station_code = cells[0].strip()
            if not station_code or not station_code.isdigit():
                continue  # skip summary/blank rows
            station_name = cells[1].strip()
            station_name_en = self.translate(station_name)

            # station-wide statistics (same on every year row)
            normal_mmdd = _safe_int(cells, normal_ci)
            latest_mmdd = _safe_int(cells, max_val_ci)
            latest_yr = _safe_int(cells, max_yr_ci)
            earliest_mmdd = _safe_int(cells, min_val_ci)
            earliest_yr = _safe_int(cells, min_yr_ci)

            for year, ci in year_col.items():
                if year > current_year:
                    continue  # skip projected / empty future columns

                val_str = cells[ci].strip() if ci < len(cells) else ''
                if not val_str or val_str in _MISSING:
                    continue
                try:
                    obs_mmdd = int(val_str)
                except ValueError:
                    continue

                remark = ''
                ri = remark_col.get(year)
                if ri is not None and ri < len(cells):
                    remark = cells[ri].strip()

                row: dict = {
                    'species_code': species_code,
                    'station_code': station_code,
                    'year': year,
                    'species_name_en': species_name_en,
                    'station_name_en': station_name_en,
                    'observation_mmdd': obs_mmdd,
                    'observation_date': _mmdd_to_date(year, obs_mmdd),
                    'remark': remark,
                }
                if normal_mmdd is not None:
                    row['normal_value_mmdd'] = normal_mmdd
                if latest_mmdd is not None:
                    row['latest_date_mmdd'] = latest_mmdd
                if latest_yr is not None:
                    row['latest_date_year'] = latest_yr
                if earliest_mmdd is not None:
                    row['earliest_date_mmdd'] = earliest_mmdd
                if earliest_yr is not None:
                    row['earliest_date_year'] = earliest_yr

                rows.append(row)

        return rows

    # ------------------------------------------------------------------
    # Required by abstract base (unused here)
    # ------------------------------------------------------------------

    def parse_entry(self, root: ET.Element, data_url: str) -> dict | None:
        """Not used — this dataset overrides fetch() directly."""
        return None