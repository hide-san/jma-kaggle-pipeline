"""
One-time backfill: merge JMA phenological observations archive into
the existing jma-phenological-observations Kaggle dataset.

Downloads the cumulative multi-year archive (累年値) from:
  https://www.data.jma.go.jp/sakura/data/ruinenchi/ruinenchi_all.zip

Reshapes it into the same row format as the live feed pipeline, assigns
synthetic event_ids (archive_<species>_<station>_<year>), then merges
with the current Kaggle dataset and uploads a new version.

Usage:
    python scripts/backfill_phenology_archive.py
    python scripts/backfill_phenology_archive.py --dry-run

Requires KAGGLE_USERNAME and KAGGLE_API_TOKEN in the environment / .env.
"""

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config  # noqa: E402 — must come before jma_api_client imports
from jma_api_client.jma_phenology_archive import PhenologicalObservationArchive
from kaggle_uploader import KaggleUploader
from logger import get_logger

log = get_logger(__name__)

# Target dataset — same as the live feed pipeline
KAGGLE_DATASET = f"{config.KAGGLE_USERNAME}/jma-phenological-observations"
CSV_FILENAME = "jma_phenological_observations.csv"
MERGE_KEYS = ["event_id"]


def archive_to_feed_schema(archive_df) -> "pd.DataFrame":
    """
    Map archive columns to the jma-phenological-observations schema.

    Feed columns populated from archive data:
      event_id          — synthetic: "archive_<species_code>_<station_code>_<year>"
      observation_date  — ISO date derived from year + observation_mmdd
      station_code      — from archive
      station_name_en   — from archive
      species_en        — from archive (species_name_en)
      deviation_from_normal — days difference (observation − normal), where available

    All other feed columns are left as NaN; archive-specific columns are
    included as extra columns so the data is not lost.
    """
    import pandas as pd

    df = archive_df.copy()

    df["event_id"] = (
        "archive_"
        + df["species_code"].astype(str)
        + "_"
        + df["station_code"].astype(str)
        + "_"
        + df["year"].astype(str)
    )

    # Vectorised deviation: convert MMDD to day-of-year then diff
    if "normal_value_mmdd" in df.columns:
        obs_dates = pd.to_datetime(
            df["observation_date"], errors="coerce"
        )
        norm_dates = pd.to_datetime(
            df["year"].astype(str)
            + "-"
            + (df["normal_value_mmdd"] // 100).astype(str).str.zfill(2)
            + "-"
            + (df["normal_value_mmdd"] % 100).astype(str).str.zfill(2),
            errors="coerce",
        )
        df["deviation_from_normal"] = (obs_dates - norm_dates).dt.days
    else:
        df["deviation_from_normal"] = None

    df = df.rename(columns={"species_name_en": "species_en"})

    # Keep the columns that match the feed schema plus archive-specific extras
    keep = [
        "event_id", "observation_date", "station_code", "station_name_en",
        "species_en", "deviation_from_normal",
        "species_code", "year", "observation_mmdd", "remark",
        "normal_value_mmdd", "latest_date_mmdd", "latest_date_year",
        "earliest_date_mmdd", "earliest_date_year",
    ]
    return df[[c for c in keep if c in df.columns]]


def main(dry_run: bool = False) -> bool:
    kaggle = KaggleUploader()

    if not dry_run:
        if not kaggle.authenticate():
            log.error("Kaggle authentication failed — aborting.")
            return False
    else:
        log.info("DRY-RUN: skipping Kaggle authentication")

    # 1. Fetch and parse archive
    log.info("Fetching phenological observations archive …")
    archive_instance = PhenologicalObservationArchive()
    archive_df = archive_instance.fetch()
    if archive_df.empty:
        log.error("Archive returned no data — aborting.")
        return False
    log.info("Archive: %d rows across %d species",
             len(archive_df), archive_df["species_code"].nunique())

    # 2. Convert to feed schema
    log.info("Converting archive to feed schema …")
    new_df = archive_to_feed_schema(archive_df)

    # 3. Download current Kaggle dataset
    existing_df = kaggle.download_dataset(KAGGLE_DATASET, CSV_FILENAME)
    log.info("Existing Kaggle dataset: %d rows", len(existing_df))

    # 4. Merge (existing wins for duplicate event_ids — archive rows have
    #    synthetic ids so they won't collide with live feed rows)
    merged_df = kaggle.merge_data(existing_df, new_df, MERGE_KEYS)
    new_rows = len(merged_df) - len(existing_df)
    log.info("After merge: %d rows (%+d)", len(merged_df), new_rows)

    if new_rows <= 0:
        log.info("No new rows to add — dataset is already up to date.")
        return True

    # 5. Upload
    if dry_run:
        log.info("DRY-RUN: would upload %d rows to %s", len(merged_df), KAGGLE_DATASET)
        log.info("Sample new rows:\n%s", new_df.head(3).to_string())
        return True

    ok = kaggle.upload_dataset(
        KAGGLE_DATASET,
        CSV_FILENAME,
        merged_df,
        title="JMA Phenological Observations",
        description=(
            "生物季節観測 — Phenological Observation. "
            "Includes historical archive data (1953–present) from JMA cumulative records "
            "(累年値) merged with ongoing real-time feed updates (VGSK55)."
        ),
        keywords=["japan", "plants", "environment", "climate-change", "asia"],
    )
    if ok:
        kaggle.wait_until_ready(KAGGLE_DATASET)
    return ok


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill phenological archive into Kaggle")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and merge without uploading")
    args = parser.parse_args()

    success = main(dry_run=args.dry_run)
    sys.exit(0 if success else 1)
