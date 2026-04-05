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


def _split_species_phenophase(ja_name: str) -> tuple[str, str]:
    """
    Split a JMA archive species name into (species, phenophase).

    Archive names bundle both: e.g. "ウメの開花" → ("ウメ", "開花")
    We split on the LAST "の" which separates species from the event.
    If there is no "の", the whole string is returned as species.
    """
    idx = ja_name.rfind("の")
    if idx == -1:
        return ja_name, ""
    return ja_name[:idx], ja_name[idx + 1:]


def archive_to_feed_schema(archive_df, existing_df=None) -> "pd.DataFrame":
    """
    Intelligently map archive columns to the exact 25-column feed schema.

    Intelligence applied:
      - species_name split on last "の" → separate species + phenophase (ja + en)
      - station_location/publishing_office looked up from existing feed rows
        by station_code where available
      - deviation_from_normal computed from normal_value_mmdd
      - info_type_en set to "historical archive"
      - title / title_en generated from phenophase name
    """
    import pandas as pd
    from jma_api_client.translate import translate_ja_to_en

    df = archive_df.copy()

    # --- event_id ---
    df["event_id"] = (
        "archive_"
        + df["species_code"].astype(str)
        + "_"
        + df["station_code"].astype(str)
        + "_"
        + df["year"].astype(str)
    )

    # --- split species_name into species + phenophase ---
    split = df["species_name"].apply(_split_species_phenophase)
    df["species"]       = split.apply(lambda t: t[0])
    df["phenophase"]    = split.apply(lambda t: t[1])
    df["species_en"]    = df["species"].apply(translate_ja_to_en)
    df["phenophase_en"] = df["phenophase"].apply(
        lambda t: translate_ja_to_en(t) if t else None
    )

    # --- title: use the full phenophase description (= original species_name) ---
    df["title"]    = df["species_name"]
    df["title_en"] = df["species_name_en"]  # already translated full name

    # --- info_type ---
    df["info_type_en"] = "historical archive"

    # --- station_name (Japanese already in archive) ---
    # station_name_en already present

    # --- deviation_from_normal ---
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

    # --- station metadata lookup from existing feed rows ---
    # The live feed has station_location, publishing_office etc. indexed by station_code.
    # Fill those in for archive rows where the station appears in the feed.
    if existing_df is not None and not existing_df.empty:
        fill_cols = [
            "station_location", "station_location_en",
            "publishing_office", "publishing_office_en",
            "station_status", "station_status_en",
        ]
        available = [c for c in fill_cols if c in existing_df.columns]
        if available:
            station_meta = (
                existing_df[["station_code"] + available]
                .dropna(subset=["station_code"])
                .drop_duplicates(subset=["station_code"])
                .set_index("station_code")
            )
            for col in available:
                df[col] = df["station_code"].astype(str).map(
                    station_meta[col].astype(str).replace("nan", None)
                )

    # --- observation_remark ---
    df = df.rename(columns={"remark": "observation_remark"})

    # --- build output with exactly the feed columns ---
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

    # 2. Download current Kaggle dataset first (needed for station lookup)
    existing_df = kaggle.download_dataset(KAGGLE_DATASET, CSV_FILENAME)
    log.info("Existing Kaggle dataset: %d rows", len(existing_df))

    # 3. Convert to feed schema (uses existing_df for station metadata lookup)
    log.info("Converting archive to feed schema …")
    new_df = archive_to_feed_schema(archive_df, existing_df)

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
