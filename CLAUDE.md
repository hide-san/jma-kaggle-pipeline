# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**JMA-Kaggle Pipeline** is an automated data pipeline that fetches meteorological and seismic data from the Japan Meteorological Agency (JMA), merges it with existing datasets on Kaggle, and publishes updates. It runs every hour via GitHub Actions.

The pipeline ingests three datasets:
1. **Cherry Blossom Observations**: Annual cherry blossom blooming data by observation station
2. **Japan City Temperatures**: Daily AMeDAS temperature readings from weather stations
3. **Earthquake Data**: Latest seismic events reported by JMA

## Architecture

### Data Flow

```
JMA APIs → JMAApiClient (fetch)
           ↓
    KaggleUploader (download existing)
           ↓
    merge_data (combine + deduplicate)
           ↓
    KaggleUploader (upload new version)
```

### Key Modules

**`data_pipeline.py`** — Main orchestrator. Implements `run_pipeline()` which:
1. Loops through configured datasets in `config.DATASETS`
2. For each dataset: fetches new data → downloads existing → merges → uploads
3. Logs results and exits with status code (0 success, 1 failure)

**`jma_api_client.py`** — REST API client with three fetch methods:
- `fetch_cherry_blossom_data(year)`: Retrieves CSV from `data.jma.go.jp` (uses Shift-JIS encoding)
- `fetch_temperature_data()`: Queries AMeDAS latest snapshot from `jma.go.jp/bosai/amedas`
- `fetch_earthquake_data()`: Fetches JSON list from `jma.go.jp/bosai/quake`

All methods have retry logic (3 attempts, 5-second wait) via `@retry` decorator from `tenacity`.

**`kaggle_uploader.py`** — Kaggle dataset integration:
- `authenticate()`: Validates credentials from environment
- `download_dataset(kaggle_dataset, csv_filename)`: Pulls existing data (returns empty DataFrame if dataset doesn't exist yet)
- `merge_data(existing, new, merge_keys)`: Deduplicates on merge_keys, keeps newer rows on conflicts
- `upload_dataset(kaggle_dataset, csv_filename, df, description)`: Writes CSV to temp directory, creates required `dataset-metadata.json`, uploads via Kaggle API

**`config.py`** — Configuration via environment variables (`.env`):
- `KAGGLE_USERNAME`, `KAGGLE_KEY` (required)
- `JMA_BASE_URL` hardcoded
- `DATASETS` list defines which datasets to process and their merge keys
- Retry settings and log file path

**`logger.py`** — Simple logging setup: writes to both `logs/pipeline.log` and stdout at INFO level.

## Development Commands

### Install dependencies
```bash
pip install -r requirements.txt
```

### Run the full pipeline (local)
```bash
python data_pipeline.py
```
Requires `.env` file with `KAGGLE_USERNAME` and `KAGGLE_KEY` set. See `.env.example` for template.

### Run tests
```bash
pytest tests/
```
Tests are in `tests/test_pipeline.py`, focusing on `KaggleUploader.merge_data()` logic. No external dependencies required (no mocking).

### Run a single test
```bash
pytest tests/test_pipeline.py::test_merge_deduplication_new_wins -v
```

## Configuration & Environment

**`.env` file** (create from `.env.example`):
```
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key
```

**Dataset Configuration** (`config.py`):
Each entry in `DATASETS` list requires:
- `name`: Internal identifier
- `kaggle_dataset`: Owner/slug format (e.g., `username/cherry-blossom-observations`)
- `csv_filename`: File to download/upload within the dataset
- `merge_keys`: Column(s) to deduplicate on (list of strings)
- `description`: Version notes for uploads

## CI/CD

**Workflow**: `.github/workflows/daily-update.yml`
- Runs on schedule every hour (0 * * * * UTC)
- Can be triggered manually via workflow_dispatch
- On failure: uploads logs as artifact and creates GitHub issue with label `pipeline-failure`

## Testing Notes

- Tests use pandas DataFrames directly; no mocking of external APIs
- `merge_data()` tests verify deduplication, empty-case handling, and multi-key merging
- To test against live APIs, run `python data_pipeline.py` with valid Kaggle credentials

## Common Issues

- **Kaggle Auth Failed**: Verify `.env` file exists with correct credentials; Kaggle API key must be from account settings
- **Dataset Not Found**: First pipeline run for a new dataset will create it (no existing data to merge)
- **Shift-JIS Encoding**: Cherry blossom CSV uses Japanese character encoding; handled in `jma_api_client.py` with fallback
