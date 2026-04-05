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


# Exact column order of the existing jma-phenological-observations dataset
FEED_COLUMNS = [
    "event_id", "title", "title_en", "report_datetime", "info_type",
    "info_type_en", "observation_date", "phenophase", "phenophase_en",
    "phenophase_code", "station_name", "station_name_en", "station_location",
    "station_location_en", "publishing_office", "publishing_office_en",
    "species", "species_en", "condition", "station_code", "station_status",
    "station_status_en", "deviation_from_normal", "deviation_from_last_year",
    "observation_remark",
]


def archive_to_feed_schema(archive_df) -> "pd.DataFrame":
    """
    Map archive columns to the exact 25-column jma-phenological-observations
    schema. No extra columns are added; unmapped columns are left as NaN.

    Mappings:
      event_id            ← synthetic "archive_<species_code>_<station_code>_<year>"
      observation_date    ← ISO date from archive
      station_code        ← archive station_code
      station_name_en     ← archive station_name_en
      species_en          ← archive species_name_en
      species             ← archive species_name (Japanese)
      observation_remark  ← archive remark
      deviation_from_normal ← days(observation − normal), computed from normal_value_mmdd
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

    # Vectorised deviation_from_normal
    if "normal_value_mmdd" in df.columns:
        obs_dates = pd.to_datetime(df["observation_date"], errors="coerce")
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

    # Rename archive columns to feed column names
    df = df.rename(columns={
        "species_name_en": "species_en",
        "species_name":    "species",
        "remark":          "observation_remark",
    })

    # Build output with exactly the feed columns; missing ones become NaN
    out = pd.DataFrame(index=df.index)
    for col in FEED_COLUMNS:
        out[col] = df[col] if col in df.columns else None
    return out


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
