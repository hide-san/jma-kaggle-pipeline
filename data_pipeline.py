"""
Main orchestration pipeline.

Usage:
    python data_pipeline.py
"""

import os
import sys

import config
from jma_api_client import JMAApiClient
from kaggle_uploader import KaggleUploader
from logger import get_logger

log = get_logger(__name__)


def run_pipeline() -> bool:
    jma = JMAApiClient()
    kaggle = KaggleUploader()

    if not kaggle.authenticate():
        log.error("Aborting pipeline: Kaggle authentication failed.")
        return False

    # Map dataset name → fetch function
    fetchers = {
        "cherry-blossom-observations": jma.fetch_cherry_blossom_data,
        "japan-city-temperatures": jma.fetch_temperature_data,
        "japan-earthquakes": jma.fetch_earthquake_data,
        "japan-earthquakes-enhanced": jma.fetch_earthquakes_enhanced,
    }

    results: dict[str, bool] = {}

    for dataset_cfg in config.DATASETS:
        name = dataset_cfg["name"]
        log.info("=" * 60)
        log.info("Processing dataset: %s", name)

        try:
            # 1. Fetch new data from JMA
            fetch_fn = fetchers[name]
            new_df = fetch_fn()

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

            # 3. Merge
            merged_df = kaggle.merge_data(
                existing_df,
                new_df,
                dataset_cfg["merge_keys"],
            )

            # 4. Upload
            ok = kaggle.upload_dataset(
                dataset_cfg["kaggle_dataset"],
                dataset_cfg["csv_filename"],
                merged_df,
                description=dataset_cfg.get("description", ""),
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
    success = run_pipeline()
    sys.exit(0 if success else 1)
