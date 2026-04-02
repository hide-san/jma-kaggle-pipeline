"""
Kaggle dataset integration: download, merge, and upload CSVs.

Uses the Python SDK (kaggle PyPI package) for reliable programmatic access:
- Handles all auth natively (reads KAGGLE_USERNAME + KAGGLE_KEY env vars)
- No subprocess/PATH issues
- Works cross-platform (Windows, macOS, Linux)
- Official Kaggle tool for Python integrations

See: https://github.com/Kaggle/kaggle-api for SDK source
See: https://www.kaggle.com/docs/api for official documentation
"""

import json
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from logger import get_logger

if TYPE_CHECKING:
    from kaggle.api.kaggle_api_extended import KaggleApi

log = get_logger(__name__)


class KaggleUploader:
    def __init__(self):
        self._api: "KaggleApi | None" = None

    # ------------------------------------------------------------------ #
    # Authentication                                                       #
    # ------------------------------------------------------------------ #

    def authenticate(self) -> bool:
        """Authenticate with Kaggle. Returns True on success."""
        try:
            # Support both KAGGLE_KEY and KAGGLE_API_TOKEN (from .env file)
            username = os.environ.get("KAGGLE_USERNAME")
            api_key = os.environ.get("KAGGLE_KEY") or os.environ.get("KAGGLE_API_TOKEN")

            if not username or not api_key:
                log.error("Kaggle authentication failed: KAGGLE_USERNAME and KAGGLE_KEY (or KAGGLE_API_TOKEN) required")
                return False

            # Ensure KAGGLE_KEY is set for the SDK
            os.environ["KAGGLE_KEY"] = api_key

            from kaggle.api.kaggle_api_extended import KaggleApi
            api = KaggleApi()
            api.authenticate()
            self._api = api
            log.info("Kaggle authentication successful")
            return True
        except Exception as exc:
            log.error("Kaggle authentication failed: %s", exc)
            return False

    @property
    def api(self) -> "KaggleApi":
        if self._api is None:
            raise RuntimeError("Call authenticate() before using the API.")
        return self._api

    # ------------------------------------------------------------------ #
    # Download                                                             #
    # ------------------------------------------------------------------ #

    def download_dataset(self, kaggle_dataset: str, csv_filename: str) -> pd.DataFrame:
        """
        Download *csv_filename* from *kaggle_dataset* and return it as a DataFrame.
        Returns an empty DataFrame if the dataset or file does not exist yet.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                log.info("Downloading dataset: %s", kaggle_dataset)
                self.api.dataset_download_files(
                    kaggle_dataset,
                    path=tmpdir,
                    unzip=True,
                    quiet=True,
                )
                csv_path = Path(tmpdir) / csv_filename
                if not csv_path.exists():
                    log.warning("File %s not found in dataset %s", csv_filename, kaggle_dataset)
                    return pd.DataFrame()
                df = pd.read_csv(csv_path)
                log.info("Downloaded %d rows from %s", len(df), kaggle_dataset)
                return df
            except Exception as exc:
                log.warning("Could not download dataset %s: %s — treating as empty", kaggle_dataset, exc)
                return pd.DataFrame()

    # ------------------------------------------------------------------ #
    # Merge                                                                #
    # ------------------------------------------------------------------ #

    def merge_data(
        self,
        existing_df: pd.DataFrame,
        new_df: pd.DataFrame,
        merge_keys: list[str],
    ) -> pd.DataFrame:
        """
        Combine *existing_df* and *new_df*, deduplicate on *merge_keys*.
        New rows take precedence over old ones for the same key.
        """
        if existing_df.empty:
            log.info("No existing data — using new data as-is (%d rows)", len(new_df))
            return new_df

        if new_df.empty:
            log.info("No new data fetched — keeping existing data (%d rows)", len(existing_df))
            return existing_df

        combined = pd.concat([existing_df, new_df], ignore_index=True)
        # Keep the *last* occurrence so new data wins on duplicate keys
        valid_keys = [k for k in merge_keys if k in combined.columns]
        if valid_keys:
            combined = combined.drop_duplicates(subset=valid_keys, keep="last")
        else:
            log.warning("Merge keys %s not found in DataFrame columns — skipping dedup", merge_keys)

        log.info(
            "Merged: existing=%d + new=%d → combined=%d rows",
            len(existing_df), len(new_df), len(combined),
        )
        return combined

    # ------------------------------------------------------------------ #
    # Upload                                                               #
    # ------------------------------------------------------------------ #

    def upload_dataset(
        self,
        kaggle_dataset: str,
        csv_filename: str,
        df: pd.DataFrame,
        description: str = "",
    ) -> bool:
        """
        Write *df* to a temporary CSV and push it to *kaggle_dataset*.
        Automatically creates the dataset if it doesn't exist yet.
        Returns True on success.
        """
        owner, dataset_slug = kaggle_dataset.split("/", 1)

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / csv_filename
            df.to_csv(csv_path, index=False)
            log.info("Uploading %d rows to %s", len(df), kaggle_dataset)

            # Write dataset-metadata.json required by the API
            metadata = {
                "title": dataset_slug.replace("-", " ").title(),
                "id": kaggle_dataset,
                "licenses": [{"name": "CC0-1.0"}],
            }
            (Path(tmpdir) / "dataset-metadata.json").write_text(
                json.dumps(metadata, ensure_ascii=False), encoding="utf-8"
            )

            try:
                # Try to create a new version (for existing datasets)
                self.api.dataset_create_version(
                    folder=tmpdir,
                    version_notes=description or "Automated daily update",
                    quiet=True,
                    convert_to_csv=False,
                    delete_old_versions=False,
                )
                log.info("Upload successful (new version): %s", kaggle_dataset)
                return True
            except Exception as version_exc:
                # Check if dataset doesn't exist (404, 401, or "not found" error)
                exc_str = str(version_exc).lower()
                if "404" in str(version_exc) or "401" in str(version_exc) or "not found" in exc_str or "does not exist" in exc_str or "unauthorized" in exc_str:
                    log.info("Dataset not found or unauthorized, creating new dataset: %s", kaggle_dataset)
                    try:
                        # Create dataset for the first time
                        self.api.dataset_create_new(
                            folder=tmpdir,
                            dir_mode='zip',
                            quiet=True,
                        )
                        log.info("Created and uploaded dataset successfully: %s", kaggle_dataset)
                        return True
                    except Exception as create_exc:
                        log.error("Failed to create dataset %s: %s", kaggle_dataset, create_exc)
                        return False
                else:
                    # Some other error occurred
                    log.error("Upload failed for %s: %s", kaggle_dataset, version_exc)
                    return False
