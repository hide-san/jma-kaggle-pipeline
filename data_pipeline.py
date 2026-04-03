"""
Main orchestration pipeline.

Usage:
    python data_pipeline.py
    python data_pipeline.py --dry-run
    python data_pipeline.py --datasets japan-earthquakes,japan-sea-warnings
"""

import argparse
import os
import sys

import config
from jma_api_client.base import DATASET_REGISTRY
from kaggle_uploader import KaggleUploader
from logger import get_logger

log = get_logger(__name__)


def run_pipeline(dry_run: bool = False) -> bool:
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

    results: dict[str, bool] = {}

    for dataset_cfg in filtered_datasets:
        name = dataset_cfg["name"]
        log.info("=" * 60)
        log.info("Processing dataset: %s", name)

        try:
            # 1. Fetch new data from JMA using dataset class from registry
            if name not in DATASET_REGISTRY:
                log.error("Dataset '%s' not found in registry", name)
                results[name] = False
                continue

            dataset_cls = DATASET_REGISTRY[name]
            new_df = dataset_cls().fetch()

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
    for name, ok in results.items():
        status = "OK" if ok else "FAILED"
        log.info("  %s: %s", name, status)
        if not ok:
            all_ok = False

    return all_ok


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="JMA data pipeline")
    parser.add_argument("--dry-run", action="store_true", help="Simulate pipeline without uploading to Kaggle")
    parser.add_argument("--datasets", help="Comma-separated list of dataset names to process (e.g., japan-earthquake-and-seismic-information,japan-regional-sea-alert)")
    parser.add_argument("--list-datasets", action="store_true", help="List all available datasets and exit")
    args = parser.parse_args()

    # Handle --list-datasets
    if args.list_datasets:
        print("Available datasets:")
        print()
        for dataset_cfg in config.DATASETS:
            name = dataset_cfg["name"]
            desc = dataset_cfg.get("description", "")
            print(f"  {name:45} {desc}")
        print()
        sys.exit(0)

    # Handle --datasets argument (CLI override)
    if args.datasets:
        os.environ["DATASETS_FILTER"] = args.datasets

    success = run_pipeline(dry_run=args.dry_run)
    sys.exit(0 if success else 1)
