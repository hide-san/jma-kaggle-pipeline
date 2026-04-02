# JMA-Kaggle Pipeline

Automated data pipeline that fetches meteorological and seismic data from the **Japan Meteorological Agency (JMA)**, merges with existing Kaggle datasets, and publishes updates daily.

## Features

- **Multi-Source JMA Data**: Earthquakes, volcanic ash, volcano status, cherry blossoms, sea warnings/forecasts
- **Dual-Language CSVs**: Original Japanese text + English translations for all fields
- **Automated Daily Updates**: Runs at 09:00 JST via GitHub Actions
- **Intelligent Merging**: Deduplicates on configurable keys; newer data takes precedence
- **Error Handling**: Retry logic for API calls, automatic issue creation on pipeline failure
- **Logging**: Full pipeline execution logs for debugging and monitoring
- **Data Persistence**: Saves both raw API responses and parsed data locally to `data/` directory

## Data Sources

| Dataset | Code | Source | Records | Update Frequency |
|---------|------|--------|---------|------------------|
| **Earthquakes** | VXSE53 | JMA eqvol_l.xml | 108 | Real-time |
| **Enhanced Earthquakes** | VXSE53 | JMA eqvol_l.xml | 108 | Real-time |
| **Volcanic Ash Forecasts** | VFVO53 | JMA eqvol_l.xml | 896 | Real-time |
| **Volcano Status** | VFVO51 | JMA eqvol_l.xml | 24 | Real-time |
| **Cherry Blossom Observations** | VGSK55 | JMA other_l.xml | 68 | Seasonal (Mar-May) |
| **Sea Warnings** | VPCU51 | JMA other_l.xml | 612 | Real-time |
| **Sea Forecasts** | VPCY51 | JMA other_l.xml | 336 | Real-time |

## Prerequisites

- Python 3.11+
- [Kaggle API credentials](https://www.kaggle.com/settings/account)
- Write access to Kaggle datasets you want to update

## Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/jma-kaggle-pipeline.git
cd jma-kaggle-pipeline
```

### 2. Set Up Environment
```bash
cp .env.example .env
```

Edit `.env` with your Kaggle credentials:
```env
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Run the Pipeline
```bash
python data_pipeline.py
```

## Configuration

Edit `config.py` to customize:

- **Dataset mappings**: Add/remove datasets in `DATASETS` list
- **Retry behavior**: Adjust `RETRY_ATTEMPTS` and `RETRY_WAIT_SECONDS`
- **Logging**: Change `LOG_FILE` location

### Dataset Configuration Example

Each dataset requires:
```python
{
    "name": "cherry-blossom-observations",
    "kaggle_dataset": "your_username/cherry-blossom-observations",
    "csv_filename": "cherry_blossom.csv",
    "merge_keys": ["year", "station_no"],
    "description": "Cherry blossom blooming observations by JMA",
}
```

- **merge_keys**: Columns to deduplicate on (new data overwrites old)
- **kaggle_dataset**: Must follow `username/dataset-slug` format

## Testing

Run all tests:
```bash
pytest tests/
```

Run a specific test:
```bash
pytest tests/test_pipeline.py::test_merge_deduplication_new_wins -v
```

Tests focus on merge logic and handle edge cases like missing keys and empty datasets.

## Translation Features

All generated CSV files include dual-language support:

- **Original Japanese columns**: Preserve authentic JMA data
- **English translation columns** (suffixed `_en`): Machine-translated using deep_translator

Example:
```
earthquakes.csv:
  epicentre: "宮城県沖"
  epicentre_en: "Off the coast of Miyagi Prefecture"

volcano_status.csv:
  alert_level: "レベル２（火口周辺規制）"
  alert_level_en: "Level 2 (Regulations around the crater)"
  volcano_name: "草津白根山（白根山（湯釜付近））"
  volcano_name_en: "Mt. Kusatsu Shirane (Mt. Shirane (near Yugama))"
```

Translation results are cached for performance optimization.

## How It Works

```
1. Authenticate with Kaggle
   ↓
2. For each dataset in config:
   a. Fetch new data from JMA API (with retries)
   b. Download existing data from Kaggle
   c. Merge & deduplicate
   d. Upload new version to Kaggle
   ↓
3. Log results and exit (0 = success, 1 = failure)
```

### API Details

**JMA Data Feeds** (4 XML Atom feeds):
- `regular_l.xml` (3.1 MB) - Weather forecasts & seasonal data
- `extra_l.xml` (1.5 MB) - Weather warnings & alerts
- `eqvol_l.xml` (387 KB) - **Earthquakes, volcanoes, tsunamis**
- `other_l.xml` (245 KB) - **Cherry blossoms, marine data**

**Data Type Codes** (from 気象庁防災情報XML):
| Code | Description | Records |
|------|-------------|---------|
| VXSE53 | Earthquake & seismic intensity info | 108 |
| VFVO53 | Volcanic ash forecast (regular) | 896 |
| VFVO51 | Volcano status explanation | 24 |
| VGSK55 | Biological seasonal observation (cherry blossom) | 68 |
| VPCU51 | Regional sea warnings | 612 |
| VPCY51 | Regional sea forecasts | 336 |

**Reference Documentation**:
- [JMA XML Technical Materials](https://xml.kishou.go.jp/tec_material.html) - Official JMA XML specification and technical resources
- [JMA Disaster Prevention Information XML Specification](https://xml.kishou.go.jp/jmaxml_20260129_format_v1_3_hyo1_1.pdf) - Complete list of 90+ data type codes available from JMA (as of 2026-01-29)
- [JMA Information Catalog](https://www.data.jma.go.jp/add/suishin/catalogue/catalogue.html) - Detailed descriptions of each dataset

All requests include retry logic (3 attempts, 5-second wait).

## CI/CD

GitHub Actions workflow (`.github/workflows/daily-update.yml`):
- ⏰ Runs daily at **09:00 JST** (midnight UTC)
- 🚀 Can be triggered manually via workflow dispatch
- 📋 On failure: Creates GitHub issue with pipeline logs

To set up CI/CD, add these secrets to your GitHub repository:
- `KAGGLE_USERNAME`
- `KAGGLE_KEY`

## Logging

Logs are written to `logs/pipeline.log` and console output (INFO level).

Example log output:
```
2024-03-30 12:00:15,234 [INFO] data_pipeline: Processing dataset: cherry-blossom-observations
2024-03-30 12:00:16,456 [INFO] jma_api_client: Fetching cherry blossom data: https://...
2024-03-30 12:00:18,789 [INFO] kaggle_uploader: Downloaded 50 rows from username/cherry-blossom-observations
2024-03-30 12:00:19,012 [INFO] kaggle_uploader: Merged: existing=50 + new=5 → combined=53 rows
2024-03-30 12:00:22,345 [INFO] kaggle_uploader: Upload successful: username/cherry-blossom-observations
```

## Troubleshooting

### Kaggle authentication fails
- Verify `.env` file exists with correct credentials
- Ensure Kaggle API key is from account settings (not username/password)
- Run `kaggle datasets list` to test credentials manually

### "Dataset not found" error
- First run will create the dataset automatically
- Verify `kaggle_dataset` format is `username/dataset-slug`
- Ensure you have write access to the dataset

### Character encoding issues with cherry blossom data
- The JMA CSV uses Shift-JIS encoding (handled automatically)
- If you see garbled characters, check terminal encoding settings

## Project Structure

```
jma-kaggle-pipeline/
├── data_pipeline.py                # Main orchestrator
├── kaggle_uploader.py              # Kaggle integration
├── config.py                       # Configuration
├── logger.py                       # Logging setup
├── requirements.txt                # Python dependencies
├── .env.example                    # Environment template
├── jma_api_client/                 # JMA API client (modular)
│   ├── __init__.py                 # Package exports
│   ├── earthquakes.py              # VXSE53 earthquake data extraction
│   ├── volcanoes.py                # VFVO53/VFVO51 volcanic data
│   ├── cherry_blossom.py           # VGSK55 cherry blossom observations
│   ├── sea.py                      # VPCU51/VPCY51 sea warnings & forecasts
│   ├── temperature.py              # Temperature data (discontinued)
│   ├── translate.py                # Japanese-to-English translation utilities
│   └── utils.py                    # Shared utilities (HTTP, parsing, logging)
├── tests/
│   └── test_pipeline.py            # Merge logic tests
├── .github/
│   ├── actions/python-setup/
│   │   └── action.yml              # Reusable Python setup composite action
│   └── workflows/
│       └── daily-update.yml        # GitHub Actions daily pipeline trigger
└── data/                           # Generated CSV files & raw XML caches
    ├── earthquakes.csv
    ├── earthquakes_enhanced.csv
    ├── volcanic_ash_forecasts.csv
    ├── cherry_blossom_observations.csv
    ├── volcano_status.csv
    ├── sea_warnings.csv
    ├── sea_forecasts.csv
    └── raw/                        # Cached JMA XML feeds
```

## License

[Add your license here]

## Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Support

For issues, feature requests, or questions:
- Open a [GitHub Issue](https://github.com/yourusername/jma-kaggle-pipeline/issues)
- Check existing issues and documentation

---

**Made with ❤️ for the Kaggle and JMA data communities**
