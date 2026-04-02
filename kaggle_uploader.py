"""
Kaggle dataset integration: download, merge, and upload CSVs.

Uses Kaggle 2.0.0 with the modern kagglesdk library (https://github.com/Kaggle/kaggle-cli).

Authentication: Reads KAGGLE_USERNAME and KAGGLE_API_TOKEN environment variables.

See: https://github.com/Kaggle/kaggle-cli for official Kaggle CLI
See: https://www.kaggle.com/docs/api for official documentation
"""

import json
import os
import tempfile
from pathlib import Path

import pandas as pd
from kagglesdk import KaggleClient, KaggleCredentials

from logger import get_logger

log = get_logger(__name__)


class KaggleUploader:
    def __init__(self):
        self._client = None

    # ------------------------------------------------------------------ #
    # Authentication                                                       #
    # ------------------------------------------------------------------ #

    def authenticate(self) -> bool:
        """Authenticate with Kaggle. Returns True on success."""
        try:
            username = os.environ.get("KAGGLE_USERNAME")
            # Support both KAGGLE_KEY and KAGGLE_API_TOKEN (from .env file)
            api_token = os.environ.get("KAGGLE_API_TOKEN") or os.environ.get("KAGGLE_KEY")

            if not username or not api_token:
                log.error("Kaggle authentication failed: KAGGLE_USERNAME and KAGGLE_API_TOKEN (or KAGGLE_KEY) required")
                return False

            # Create credentials and client
            credentials = KaggleCredentials(username=username, api_key=api_token)
            self._client = KaggleClient(credentials=credentials)

            # Test authentication by making a simple API call
            self._client.datasets.list(page_size=1)

            log.info("Kaggle authentication successful")
            return True
        except Exception as exc:
            log.error("Kaggle authentication failed: %s", exc)
            return False

    @property
    def client(self):
        if self._client is None:
            raise RuntimeError("Call authenticate() before using the API.")
        return self._client

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
                owner, dataset_slug = kaggle_dataset.split("/", 1)

                # Use the SDK to download the dataset
                self.client.datasets.download(
                    dataset_name=dataset_slug,
                    owner=owner,
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
            # Note: isPrivate=true ensures datasets are created as private
            metadata = {
                "title": dataset_slug.replace("-", " ").title(),
                "id": kaggle_dataset,
                "licenses": [{"name": "CC0-1.0"}],
                "isPrivate": True,  # JSON encoder converts this to 'true'
            }
            (Path(tmpdir) / "dataset-metadata.json").write_text(
                json.dumps(metadata, ensure_ascii=False), encoding="utf-8"
            )

            try:
                # Try to create a new version (for existing datasets)
                version_notes = description or "Automated daily update"
                self.client.datasets.create_version(
                    dataset_name=dataset_slug,
                    owner=owner,
                    path=tmpdir,
                    version_notes=version_notes,
                    quiet=True,
                    convert_to_csv=False,
                    delete_old_versions=False,
                )
                log.info("Upload successful (new version): %s", kaggle_dataset)
                return True
            except Exception as version_exc:
                # Check if dataset doesn't exist; if so, create it
                exc_str = str(version_exc).lower()
                if "404" in str(version_exc) or "not found" in exc_str or "does not exist" in exc_str:
                    log.info("Dataset not found, creating new dataset: %s", kaggle_dataset)
                    try:
                        # Create dataset for the first time
                        self.client.datasets.create_new(
                            dataset_name=dataset_slug,
                            owner=owner,
                            path=tmpdir,
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
