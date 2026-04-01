# CLAUDE.md

This file provides guidance for AI assistants working in this repository.

## Project Overview

This is an automated data pipeline that fetches meteorological data from the Japan Meteorological Agency (JMA) APIs and uploads it as datasets to Kaggle. It runs daily via GitHub Actions (09:00 JST).

**Three datasets managed:**
- `cherry-blossom-observations` — Annual cherry blossom blooming data by station
- `japan-city-temperatures` — Daily AMeDAS temperature observations for major cities
- `japan-earthquakes` — Latest earthquake data from JMA

## Repository Structure

```
jma-kaggle-pipeline/
├── .github/workflows/daily-update.yml  # Scheduled CI/CD pipeline
├── tests/test_pipeline.py              # Unit tests (pytest)
├── .env.example                        # Required environment variables template
├── config.py                           # Dataset configs, credentials, retry settings
├── data_pipeline.py                    # Entry point — orchestrates the full pipeline
├── jma_api_client.py                   # JMA API client (fetch cherry blossom, temp, quake)
├── kaggle_uploader.py                  # Kaggle auth, download, merge, upload
├── logger.py                           # Centralized logging setup
└── requirements.txt                    # Pinned Python dependencies
```

## Development Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure credentials** — copy `.env.example` to `.env` and fill in:
   ```
   KAGGLE_USERNAME=your_kaggle_username
   KAGGLE_KEY=your_kaggle_api_key
   ```

3. **Run the pipeline:**
   ```bash
   python data_pipeline.py
   ```

4. **Run tests:**
   ```bash
   pytest tests/
   ```

## Key Files and Their Roles

| File | Purpose |
|------|---------|
| `data_pipeline.py` | Orchestrates the full pipeline: auth → fetch → download → merge → upload |
| `jma_api_client.py` | HTTP client for JMA APIs; all fetch methods return pandas DataFrames |
| `kaggle_uploader.py` | Kaggle API wrapper; `merge_data()` deduplicates with new-data-wins semantics |
| `config.py` | Single source of truth for dataset definitions, API URLs, retry settings |
| `logger.py` | Call `get_logger(__name__)` in each module for consistent logging |

## Code Conventions

- **Python 3.11**, type hints using `|` union syntax (not `Optional`/`Union`)
- **PEP 8** naming: `snake_case` for functions/variables, `PascalCase` for classes, `UPPER_CASE` for constants
- Private helper methods prefixed with `_` (e.g., `_parse_latlon()`)
- Docstrings on all public classes and methods
- Module-level loggers via `logger.get_logger(__name__)`

## Data Flow

```
JMA APIs → jma_api_client.py → DataFrame
                                    ↓
Kaggle existing dataset → kaggle_uploader.merge_data() → merged DataFrame
                                    ↓
                         kaggle_uploader.upload_dataset()
```

`merge_data()` concatenates old and new DataFrames, deduplicates on `merge_keys` (defined per dataset in `config.py`), and new rows always win over existing rows with the same keys.

## Dataset Configuration

Each dataset entry in `config.DATASETS` has:
```python
{
    "name": str,             # human-readable identifier
    "kaggle_dataset": str,   # "{username}/{dataset-slug}"
    "csv_filename": str,     # filename inside the Kaggle dataset
    "merge_keys": list[str], # columns used for deduplication
    "description": str,      # used in Kaggle dataset metadata
}
```

To add a new dataset, add an entry to the `DATASETS` list in `config.py` and implement a corresponding fetch method in `JMAApiClient`.

## Error Handling Conventions

- All API calls use `@retry` from `tenacity` (3 attempts, 5-second wait) — configured via `RETRY_ATTEMPTS` and `RETRY_WAIT_SECONDS` in `config.py`
- API failures return empty DataFrames; the pipeline logs errors and continues with remaining datasets
- The pipeline exit code reflects overall success (`0`) or partial/full failure (`1`)
- Logs go to both `logs/pipeline.log` and stdout

## CI/CD

The pipeline runs automatically via `.github/workflows/daily-update.yml`:
- **Schedule:** daily at 00:00 UTC (09:00 JST)
- **Manual trigger:** `workflow_dispatch` in the GitHub Actions UI
- **On failure:** uploads `logs/pipeline.log` as artifact and creates a GitHub issue labeled `pipeline-failure`
- **Secrets required:** `KAGGLE_USERNAME` and `KAGGLE_KEY` must be set in repository secrets

## Testing

Tests live in `tests/test_pipeline.py` and use **pytest**. Current coverage focuses on `KaggleUploader.merge_data()` with edge cases: empty inputs, deduplication, missing key columns, composite keys.

When adding new fetch methods or data transformations, add corresponding unit tests using mocked HTTP responses (do not make real API calls in tests).

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `KAGGLE_USERNAME` | Yes | Kaggle account username |
| `KAGGLE_KEY` | Yes | Kaggle API key |

Loaded via `python-dotenv` from `.env` at startup. In CI, these are injected from GitHub repository secrets.
