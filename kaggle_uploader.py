"""
Kaggle dataset integration: download, merge, and upload CSVs.

Uses the official Kaggle CLI (https://github.com/Kaggle/kaggle-cli) via
direct command invocation for reliable dataset operations.

Authentication: Requires KAGGLE_USERNAME and KAGGLE_API_TOKEN environment variables.

See: https://github.com/Kaggle/kaggle-cli for official Kaggle CLI
See: https://www.kaggle.com/docs/api for official documentation
"""

import json
import os
import subprocess
import sys
import tempfile
import time
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
            api_token = os.environ.get("KAGGLE_API_TOKEN")

            if not username or not api_token:
                log.error("Kaggle authentication failed: KAGGLE_USERNAME and KAGGLE_API_TOKEN required")
                return False

            # Ensure environment variables are set for Kaggle CLI
            os.environ["KAGGLE_USERNAME"] = username
            os.environ["KAGGLE_API_TOKEN"] = api_token

            log.info("Kaggle authentication successful")
            return True
        except Exception as exc:
            log.error("Kaggle authentication failed: %s", exc)
            return False

    def _run_kaggle_command(self, cmd_args: list[str]) -> tuple[int, str, str]:
        """
        Run a kaggle CLI command.
        Returns (returncode, stdout, stderr).
        """
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
        # Keep the *first* occurrence so existing (published) data is not modified
        valid_keys = [k for k in merge_keys if k in combined.columns]
        if valid_keys:
            combined = combined.drop_duplicates(subset=valid_keys, keep="first")
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
        keywords: list[str] | None = None,
        subtitle: str = "",
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
            if description:
                metadata["description"] = description
            if subtitle:
                metadata["subtitle"] = subtitle
            if keywords:
                metadata["keywords"] = keywords
            (Path(tmpdir) / "dataset-metadata.json").write_text(
                json.dumps(metadata, ensure_ascii=False), encoding="utf-8"
            )

            # Try to create a new dataset first
            returncode, stdout, stderr = self._run_kaggle_command([
                "datasets",
                "create",
                "--path",
                tmpdir,
                "--dir-mode",
                "zip",
            ])
            output = (stdout + stderr).lower()
            create_ok = returncode == 0 and "error" not in output

            if create_ok:
                log.info("Created new dataset successfully: %s", kaggle_dataset)
                return True

            # Dataset exists, try to create a new version
            log.info("Dataset exists — adding new version: %s", kaggle_dataset)
            version_notes = description or "Automated daily update"
            returncode, stdout, stderr = self._run_kaggle_command([
                "datasets",
                "version",
                "--path",
                tmpdir,
                "-m",
                version_notes,
                "--dir-mode",
                "zip",
            ])

            if returncode == 0:
                log.info("Upload successful (new version): %s", kaggle_dataset)
                return True

            log.error(
                "Upload failed for %s (rc=%d)\n  stdout: %s\n  stderr: %s",
                kaggle_dataset, returncode, stdout.strip(), stderr.strip(),
            )
            return False

    # ------------------------------------------------------------------ #
    # Status Polling                                                       #
    # ------------------------------------------------------------------ #

    def wait_until_ready(
        self,
        kaggle_dataset: str,
        poll_interval_sec: int = 30,
        timeout_sec: int = 600,
    ) -> bool:
        """
        Poll the Kaggle API to check if dataset has completed processing.
        Returns True when dataset reaches "ready" state, False on error or timeout.
        """
        start = time.monotonic()

        while True:
            returncode, stdout, stderr = self._run_kaggle_command([
                "datasets",
                "status",
                kaggle_dataset,
            ])
            output = (stdout + stderr).lower()

            if "ready" in output:
                log.info("Dataset is ready: %s", kaggle_dataset)
                return True
            if "error" in output:
                log.error("Dataset processing failed for %s: %s", kaggle_dataset, output)
                return False
            if time.monotonic() - start > timeout_sec:
                log.error("Timed out waiting for %s to become ready", kaggle_dataset)
                return False

            log.info("Dataset %s not ready yet — waiting %ds", kaggle_dataset, poll_interval_sec)
            time.sleep(poll_interval_sec)
