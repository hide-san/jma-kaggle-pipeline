"""
Kaggle dataset integration: download, merge, and upload CSVs.

Uses the official Kaggle CLI (subprocess calls) instead of:
- Python SDK: Heavier dependency with more complex auth management
- REST API: Requires manual header construction and auth handling

The CLI approach is preferred because:
1. Official tool - direct from Kaggle team, fewer abstraction layers
2. Simpler auth - reads KAGGLE_USERNAME/KAGGLE_KEY env vars directly
3. No type complexity - subprocess is standard library, no special imports
4. Easier to debug - CLI output maps 1:1 to user documentation
5. Future-proof - CLI is the official interface going forward

See: https://github.com/Kaggle/kaggle-cli for official Kaggle CLI
See: https://www.kaggle.com/docs/api for official documentation
"""

import json
import os
import subprocess
import tempfile
from pathlib import Path

import pandas as pd

from logger import get_logger

log = get_logger(__name__)


class KaggleUploader:
    def __init__(self):
        pass

    # ------------------------------------------------------------------ #
    # Authentication                                                       #
    # ------------------------------------------------------------------ #

    def authenticate(self) -> bool:
        """Authenticate with Kaggle. Checks that credentials are available. Returns True on success."""
        try:
            username = os.environ.get("KAGGLE_USERNAME")
            api_key = os.environ.get("KAGGLE_KEY")

            if not username or not api_key:
                log.error("Kaggle authentication failed: KAGGLE_USERNAME and KAGGLE_KEY environment variables required")
                return False

            log.info("Kaggle authentication successful")
            return True
        except Exception as exc:
            log.error("Kaggle authentication failed: %s", exc)
            return False

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
                cmd = [
                    "kaggle",
                    "datasets",
                    "download",
                    "-d",
                    kaggle_dataset,
                    "-p",
                    tmpdir,
                    "--unzip",
                    "-q",
                ]
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    env=os.environ.copy(),
                )
                if result.returncode != 0:
                    log.warning("Could not download dataset %s: %s — treating as empty", kaggle_dataset, result.stderr)
                    return pd.DataFrame()

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

            # Write dataset-metadata.json required by the CLI
            metadata = {
                "title": dataset_slug.replace("-", " ").title(),
                "id": kaggle_dataset,
                "licenses": [{"name": "CC0-1.0"}],
            }
            (Path(tmpdir) / "dataset-metadata.json").write_text(
                json.dumps(metadata, ensure_ascii=False), encoding="utf-8"
            )

            # Try to create a new version (for existing datasets)
            version_notes = description or "Automated daily update"
            version_cmd = [
                "kaggle",
                "datasets",
                "version",
                "-p",
                tmpdir,
                "-m",
                version_notes,
                "--keep-tabular",
                "-q",
            ]
            result = subprocess.run(
                version_cmd,
                capture_output=True,
                text=True,
                env=os.environ.copy(),
            )

            if result.returncode == 0:
                log.info("Upload successful (new version): %s", kaggle_dataset)
                return True

            # Check if dataset doesn't exist; if so, create it
            stderr_lower = result.stderr.lower()
            if "404" in result.stderr or "not found" in stderr_lower or "does not exist" in stderr_lower:
                log.info("Dataset not found, creating new dataset: %s", kaggle_dataset)
                create_cmd = [
                    "kaggle",
                    "datasets",
                    "create",
                    "-p",
                    tmpdir,
                    "-r",
                    "zip",
                    "-q",
                ]
                result = subprocess.run(
                    create_cmd,
                    capture_output=True,
                    text=True,
                    env=os.environ.copy(),
                )
                if result.returncode == 0:
                    log.info("Created and uploaded dataset successfully: %s", kaggle_dataset)
                    return True
                else:
                    log.error("Failed to create dataset %s: %s", kaggle_dataset, result.stderr)
                    return False
            else:
                # Some other error occurred
                log.error("Upload failed for %s: %s", kaggle_dataset, result.stderr)
                return False
