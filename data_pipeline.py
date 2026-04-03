"""
Main orchestration pipeline.

Usage:
    python data_pipeline.py
    python data_pipeline.py --dry-run
    python data_pipeline.py --dry-run --preview
    python data_pipeline.py --datasets japan-earthquakes,japan-sea-warnings
"""

import argparse
import os
import sys
import time

import config
from jma_api_client.base import DATASET_REGISTRY, fetch_all_feeds
from kaggle_uploader import KaggleUploader
from logger import get_logger

log = get_logger(__name__)


def run_pipeline(dry_run: bool = False, preview: bool = False, skip_feed_fetch: bool = False) -> bool:
    kaggle = KaggleUploader()

    if not dry_run:
        if not kaggle.authenticate():
            log.error("Aborting pipeline: Kaggle authentication failed.")
            return False
    else:
        log.info("DRY-RUN mode: Skipping Kaggle authentication")

    # Filter datasets if DATASETS_FILTER env var is set (for parallel CI/CD runs)
    datasets_filter = os.environ.get("DATASETS_FILTER")
    filtered_datasets = config.DATASETS
    if datasets_filter:
        filter_names = set(datasets_filter.split(","))
        filtered_datasets = [d for d in config.DATASETS if d["name"] in filter_names]
        log.info("Filtering datasets: %s", ", ".join(filter_names))

    if dry_run:
        log.info("DRY-RUN mode enabled: Fetching and merging data, but NOT uploading to Kaggle")

    if skip_feed_fetch:
        log.info("Skipping JMA feed fetch (using pre-cached feeds in %s)", config.RAW_DATA_DIR)
    else:
        # Download all JMA Atom feeds once before processing any dataset.
        # Every dataset class reads from the local feed cache; without this step
        # the cache is empty in CI and all fetches silently return empty DataFrames.
        log.info("Downloading JMA Atom feeds...")
        fetch_all_feeds()

    results: dict[str, bool] = {}
    metrics: dict[str, dict] = {}
    pipeline_start = time.time()

    for dataset_cfg in filtered_datasets:
        name = dataset_cfg["name"]
        log.info("=" * 60)
        log.info("Processing dataset: %s", name)
        dataset_start = time.time()

        try:
            # 1. Fetch new data from JMA using dataset class from registry
            if name not in DATASET_REGISTRY:
                log.error("Dataset '%s' not found in registry", name)
                results[name] = False
                continue

            dataset_cls = DATASET_REGISTRY[name]
            dataset_instance = dataset_cls()
            new_df = dataset_instance.fetch()

            fetch_time = time.time() - dataset_start
            metrics[name] = {
                "rows_fetched": len(new_df),
                "fetch_time_sec": fetch_time,
                "throughput_rows_per_sec": len(new_df) / fetch_time if fetch_time > 0 else 0,
            }

            # Show preview if requested
            if preview and not new_df.empty:
                log.info("Preview of %s (first 3 rows):", name)
                log.info("Columns: %s", list(new_df.columns))
                log.info("Shape: %d rows x %d columns", len(new_df), len(new_df.columns))
                for idx, row in new_df.head(3).iterrows():
                    log.info("  Row %d: %s", idx, row.to_dict())

            # 1.5. Save parsed data locally
            os.makedirs(config.DATA_DIR, exist_ok=True)
            parsed_path = os.path.join(config.DATA_DIR, dataset_cfg["csv_filename"])
            new_df.to_csv(parsed_path, index=False)
            log_path = parsed_path.replace(os.sep, '/')
            log.info("Saved parsed data to %s", log_path)

            # 2. Download current Kaggle dataset
            existing_df = kaggle.download_dataset(
                dataset_cfg["kaggle_dataset"],
                dataset_cfg["csv_filename"],
            )

            # 3. Check if there's new data before merging
            if new_df.empty:
                log.info("No new data fetched for %s — skipping upload to Kaggle", name)
                results[name] = True
                continue

            # 4. Merge
            merged_df = kaggle.merge_data(
                existing_df,
                new_df,
                dataset_cfg["merge_keys"],
            )

            # 5. Upload
            if dry_run:
                log.info("DRY-RUN: Would upload %d rows to %s", len(merged_df), dataset_cfg["kaggle_dataset"])
                ok = True
            else:
                ok = kaggle.upload_dataset(
                    dataset_cfg["kaggle_dataset"],
                    dataset_cfg["csv_filename"],
                    merged_df,
                    description=dataset_cfg.get("description", ""),
                    keywords=dataset_cfg.get("keywords", []),
                    subtitle=dataset_cfg.get("subtitle", ""),
                )

                # 6. Wait for Kaggle to process the dataset
                if ok:
                    ok = kaggle.wait_until_ready(
                        dataset_cfg["kaggle_dataset"],
                        poll_interval_sec=30,
                        timeout_sec=600,
                    )
            results[name] = ok

        except Exception as exc:
            log.exception("Unhandled error while processing %s: %s", name, exc)
            results[name] = False

    # Summary
    log.info("=" * 60)
    log.info("Pipeline summary:")
    all_ok = True
    total_rows = 0
    total_time = time.time() - pipeline_start

    for name, ok in results.items():
        status = "OK" if ok else "FAILED"
        if name in metrics:
            rows = metrics[name].get("rows_fetched", 0)
            fetch_time = metrics[name].get("fetch_time_sec", 0)
            total_rows += rows
            log.info(
                "  %s: %s (%d rows, %.2fs)",
                name, status, rows, fetch_time
            )
        else:
            log.info("  %s: %s", name, status)
        if not ok:
            all_ok = False

    # Performance metrics
    if metrics:
        log.info("=" * 60)
        log.info("Performance metrics:")
        log.info("  Total rows fetched: %d", total_rows)
        log.info("  Total pipeline time: %.2f seconds", total_time)
        log.info("  Average time per dataset: %.2f seconds", total_time / len(results))

        # Find slowest datasets
        slowest = sorted(
            metrics.items(),
            key=lambda x: x[1].get("fetch_time_sec", 0),
            reverse=True
        )[:3]
        if slowest:
            log.info("  Slowest datasets:")
            for name, data in slowest:
                log.info(
                    "    %s: %.2f seconds",
                    name, data.get("fetch_time_sec", 0)
                )

    return all_ok


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="JMA data pipeline")
    parser.add_argument("--dry-run", action="store_true", help="Simulate pipeline without uploading to Kaggle")
    parser.add_argument("--preview", action="store_true", help="Show sample data from each dataset (requires --dry-run)")
    parser.add_argument("--datasets", help="Comma-separated list of dataset names to process (e.g., japan-earthquake-and-seismic-information,japan-regional-sea-alert)")
    parser.add_argument("--list-datasets", action="store_true", help="List all available datasets and exit")
    parser.add_argument("--skip-feed-fetch", action="store_true", help="Skip downloading JMA feeds (use pre-cached files in data/raw/)")
    args = parser.parse_args()

    # Handle --list-datasets
    if args.list_datasets:
        print("Available datasets:")
        print()
        print(f"{'#':3} {'Dataset Name':50} {'Feed':15} {'Type Codes':30}")
        print("-" * 100)
        for i, dataset_cfg in enumerate(config.DATASETS, 1):
            name = dataset_cfg["name"]
            # Try to get metadata from registry
            if name in DATASET_REGISTRY:
                dataset_cls = DATASET_REGISTRY[name]
                meta = dataset_cls().get_metadata()
                feed = meta.get("feed", "?")
                codes = ", ".join(meta.get("type_codes", []))
            else:
                feed = "?"
                codes = "?"
            print(f"{i:3} {name:50} {feed:15} {codes:30}")
        print()
        print(f"Total: {len(config.DATASETS)} datasets")
        print()
        sys.exit(0)

    # Handle --datasets argument (CLI override)
    if args.datasets:
        os.environ["DATASETS_FILTER"] = args.datasets

    success = run_pipeline(dry_run=args.dry_run, preview=args.preview and args.dry_run, skip_feed_fetch=args.skip_feed_fetch)
    sys.exit(0 if success else 1)
