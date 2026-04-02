"""
Kaggle dataset integration: download, merge, and upload CSVs.

Uses the official Kaggle CLI (https://github.com/Kaggle/kaggle-cli) via
subprocess calls for reliable, cross-platform dataset operations.

Authentication: Reads KAGGLE_API_TOKEN environment variable (from .env or system).
CLI invocation: Calls 'kaggle' command via subprocess.

See: https://github.com/Kaggle/kaggle-cli for official CLI
See: https://www.kaggle.com/docs/api for official documentation
"""

import json
import os
import shutil
import subprocess
import sys
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
            # Support both KAGGLE_KEY and KAGGLE_API_TOKEN (from .env file)
            api_token = os.environ.get("KAGGLE_API_TOKEN") or os.environ.get("KAGGLE_KEY")

            if not username or not api_token:
                log.error("Kaggle authentication failed: KAGGLE_USERNAME and KAGGLE_API_TOKEN (or KAGGLE_KEY) required")
                return False

            # Ensure both KAGGLE_API_TOKEN and KAGGLE_KEY are set in environment for CLI
            os.environ["KAGGLE_API_TOKEN"] = api_token
            os.environ["KAGGLE_KEY"] = api_token

            log.info("Kaggle authentication successful")
            return True
        except Exception as exc:
            log.error("Kaggle authentication failed: %s", exc)
            return False

    def _run_kaggle_command(self, cmd_args: list[str]) -> tuple[int, str, str]:
        """
        Run a kaggle CLI command and return (returncode, stdout, stderr).
        """
        try:
            result = subprocess.run(
                ["kaggle"] + cmd_args,
                capture_output=True,
                text=True,
                env=os.environ.copy(),
            )
            return result.returncode, result.stdout, result.stderr
        except FileNotFoundError:
            # If 'kaggle' is not in PATH, try via python -m
            log.debug("'kaggle' not in PATH, trying via python -m kaggle.cli")
            try:
                result = subprocess.run(
                    [sys.executable, "-m", "kaggle.cli"] + cmd_args,
                    capture_output=True,
                    text=True,
                    env=os.environ.copy(),
                )
                return result.returncode, result.stdout, result.stderr
            except Exception as e:
                log.error("Failed to run kaggle command: %s", e)
                return 1, "", str(e)

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
                returncode, stdout, stderr = self._run_kaggle_command([
                    "datasets",
                    "download",
                    "-d",
                    kaggle_dataset,
                    "-p",
                    tmpdir,
                    "--unzip",
                    "-q",
                ])

                if returncode != 0:
                    log.warning("Could not download dataset %s: %s — treating as empty", kaggle_dataset, stderr)
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

            # Try to create a new version (for existing datasets)
            version_notes = description or "Automated daily update"
            returncode, stdout, stderr = self._run_kaggle_command([
                "datasets",
                "version",
                "-p",
                tmpdir,
                "-m",
                version_notes,
                "--keep-tabular",
                "-q",
            ])

            if returncode == 0:
                log.info("Upload successful (new version): %s", kaggle_dataset)
                return True

            # Check if dataset doesn't exist; if so, create it
            stderr_lower = stderr.lower()
            if "404" in stderr or "not found" in stderr_lower or "does not exist" in stderr_lower:
                log.info("Dataset not found, creating new dataset: %s", kaggle_dataset)
                returncode, stdout, stderr = self._run_kaggle_command([
                    "datasets",
                    "create",
                    "-p",
                    tmpdir,
                    "-r",
                    "zip",
                    "-q",
                ])
                if returncode == 0:
                    log.info("Created and uploaded dataset successfully: %s", kaggle_dataset)
                    return True
                else:
                    log.error("Failed to create dataset %s: %s", kaggle_dataset, stderr)
                    return False
            else:
                # Some other error occurred
                log.error("Upload failed for %s: %s", kaggle_dataset, stderr)
                return False
