# JMA-Kaggle Pipeline

Automated data pipeline that fetches meteorological, seismic, and volcanic data from the **Japan Meteorological Agency (JMA)**, merges with existing Kaggle datasets, and publishes updates **hourly**.

Manages **33 JMA datasets** across earthquakes, volcanoes, tsunamis, typhoons, weather hazards, seasonal forecasts, and marine data.

## Features

- **Plugin Registry Architecture**: Extensible design with automatic dataset discovery and registration
- **Dual-Language CSVs**: Japanese text + English translations for all fields
- **Hourly Automated Updates**: Runs every hour via GitHub Actions (5 parallel jobs by category)
- **Append-Only Data Model**: Preserves existing published data; deduplicates on configurable merge keys
- **Intelligent Error Handling**: Retry logic, automatic GitHub issues on failure, artifact logging
- **Performance Metrics**: Tracks fetch time, throughput, and identifies bottlenecks
- **Dry-Run & Preview Mode**: Test pipeline without uploading; preview data before commit
- **Comprehensive Logging**: Full execution logs with per-dataset metrics

## Datasets (33 Total)

### 1. Earthquakes & Seismic (7 datasets)
- **JMA Tsunami Warning Advisory Forecast** (VTSE41) - Coastal tsunami alerts with estimated wave heights and arrival times by region
- **JMA Tsunami Information** (VTSE51) - Observed tsunami wave heights and arrival times from coastal monitoring stations
- **JMA Earthquake Early Warning** (VXSE43/VXSE44) - Real-time EEW predictions with automated broadcast alerts for hazard mitigation
- **JMA Seismic Intensity Flash Report** (VXSE51) - Fast seismic intensity reports with per-prefecture maximum intensity values
- **JMA Hypocenter and Seismic Intensity** (VXSE53) - Earthquake hypocenter, magnitude (Mjma), and per-prefecture seismic intensity
- **JMA Earthquake Activity Status** (VXSE56) - Seismic activity summaries with aftershock stats and ongoing trend reports
- **JMA Seismic Observation Information** (VXSE60/61/62) - Seismic network status with earthquake counts and long-period oscillations

### 2. Volcanoes & Eruptions (6 datasets)
- **JMA Eruption Warning and Forecast** (VFVO50) - Eruption warning levels (1-5) with expected volcanic hazards and impact areas
- **JMA Volcanic Situation Explanation** (VFVO51) - Volcano alert level (1-5) explanations with activity and prevention summaries
- **JMA Volcanic Eruption Observation Report** (VFVO52) - Detailed eruption observations with activity characteristics and plume data
- **JMA Volcanic Ash Forecast** (VFVO53) - 6-hour window volcanic ash dispersion forecasts with affected regions and dates
- **JMA Eruption Flash Report** (VFVO56) - Rapid eruption notifications with eruption type and affected volcano
- **JMA Estimated Plume Direction Report** (VFVO60) - Volcanic ash plume direction forecasts with time-stamped directional predictions

### 3. Tsunamis & Marine (5 datasets)
- **JMA General Tidal Information** (VMCJ50) - National tidal forecasts with tidal anomaly observations and event details
- **JMA Regional Tidal Information** (VMCJ51) - Tidal predictions with high/low tide times and heights by coastal region
- **JMA Regional Sea Warning** (VPCU51) - Regional maritime hazard alerts with warning types and affected coastal regions
- **JMA Regional Sea Forecast** (VPCY51) - Maritime forecasts with predicted wave heights, wind, and conditions by region
- **JMA General Marine Warning** (VPZU50/51/54) - Open ocean marine warnings with sea state and wind hazard information

### 4. Weather & Hazards (5 datasets)
- **JMA Tornado Watch Information** (VPHW50/VPHW51) - Tornado watch alerts with affected areas and atmospheric conditions
- **JMA Record Short-time Heavy Rain Information** (VPOA50) - Exceptional rainfall event alerts with affected regions and intensity
- **JMA Weather Warning and Advisory** (VPWW53-61) - Severe weather alerts (rain, snow, wind, waves, floods) by region and type
- **JMA Designated River Flood Forecast** (VXKO50-89) - River flood forecasts by designated river with stage predictions and timing
- **JMA Landslide Warning Information** (VXWW50) - Landslide risk alerts with trigger conditions and affected areas

### 5. Typhoons & Forecasts (5 datasets)
- **JMA Regional Seasonal Forecast** (VPCK50) - Regional 1-month, 3-month, and seasonal forecasts by district
- **JMA General Typhoon Information** (VPTI50) - Typhoon position, intensity (pressure/wind), and forecast track information
- **JMA General Typhoon Information Standardized** (VPTI51) - Standardized-format typhoon data with pressure and wind speed
- **JMA General Typhoon Information Detailed** (VPTI52) - Detailed typhoon forecasts with extended track and impact predictions
- **JMA General Seasonal Forecast** (VPZK50) - 1-month, 3-month, and warm/cold season temperature and precipitation forecasts

### 6. Phenology & Observations (3 datasets)
- **JMA Seasonal Observation** (VGSK50) - Seasonal tree and plant phenophase observations with station data
- **JMA Phenological Observations** (VGSK55) - Biological phenophase observations (plants, insects, birds) by station and date
- **JMA Special Weather Report** (VGSK60) - Notable weather phenomena reports with locations and descriptions

### 7. Informational Notices (2 datasets)
- **JMA Earthquake and Tsunami Notice** (VZSE40) - Explanatory notices and clarifications about earthquake/tsunami events
- **JMA Volcano Notice** (VZVO40) - Explanatory notices and announcements about volcanic activity

## Prerequisites

- Python 3.11+
- [Kaggle API credentials](https://www.kaggle.com/settings/account)
- Write access to Kaggle datasets you want to update

## Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/hide-san/jma-kaggle-pipeline.git
cd jma-kaggle-pipeline
```

### 2. Set Up Environment
```bash
cp .env.example .env
```

Edit `.env` with your Kaggle credentials:
```env
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_API_TOKEN=your_kaggle_api_key
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Run the Pipeline
```bash
# Full pipeline run (fetches, merges, uploads)
python scripts/data_pipeline.py

# Dry-run mode (test without uploading)
python scripts/data_pipeline.py --dry-run

# Dry-run with data preview
python scripts/data_pipeline.py --dry-run --preview

# Process specific datasets only
python scripts/data_pipeline.py --datasets japan-earthquakes,japan-typhoons

# List all available datasets
python scripts/data_pipeline.py --list-datasets
```

## Configuration

### Plugin Registry Architecture

Datasets are automatically discovered via the `@register_dataset` decorator in `jma_api_client/`:

```python
@register_dataset
class EarthquakeIntensityInfo(JMADatasetBase):
    NAME = "japan-earthquake-and-seismic-information"
    FEED_NAME = "eqvol_l.xml"
    TYPE_CODES = ("VXSE53",)
    # ... implementation
```

No manual config needed—just define the class and it's automatically:
1. Registered in `DATASET_REGISTRY`
2. Available in the pipeline
3. Exportable from `config.py`

### Environment Configuration

Edit `.env` (or set environment variables):
- `KAGGLE_USERNAME` - Kaggle account name
- `KAGGLE_API_TOKEN` - API token (from account settings)
- `DATASETS_FILTER` - Comma-separated dataset names (for CI/CD parallelization)

Edit `config.py` to customize:
- `RETRY_ATTEMPTS` - HTTP retry count (default: 3)
- `RETRY_WAIT_SECONDS` - Wait between retries (default: 5)
- `DATA_DIR` - Where to save parsed CSVs (default: `data/`)

## Testing

Run all tests:
```bash
pytest tests/
```

Run a specific test:
```bash
pytest tests/test_pipeline.py::test_merge_deduplication -v
```

Tests focus on merge logic with edge cases (missing keys, empty datasets, multi-key deduplication).

## Translation Features

All CSV files include dual-language columns:

```
earthquakes.csv:
  epicentre: "宮城県沖"
  epicentre_en: "Off the coast of Miyagi Prefecture"
  
volcano_status.csv:
  alert_level: "レベル２（火口周辺規制）"
  alert_level_en: "Level 2 (Regulations around the crater)"
  volcano_name: "草津白根山"
  volcano_name_en: "Mt. Kusatsu Shirane"
```

Translation is cached for performance.

## How It Works

```
1. Authenticate with Kaggle
   ↓
2. Fetch and cache all JMA Data Feeds (4 XML files)
   ↓
3. For each dataset (in parallel groups):
   a. Parse XML entries into DataFrame
   b. Download existing data from Kaggle
   c. Merge & deduplicate (keep='first' → preserve published data)
   d. Upload new version to Kaggle
   e. Wait until dataset is ready
   ↓
4. Log performance metrics and results
   ↓
5. Exit with status (0 = all OK, 1 = any failure)
```

## JMA Data Feeds

All data sourced from 4 JMA XML Atom feeds cached locally:

| Feed | Size | Content |
|------|------|---------|
| `regular_l.xml` | ~3.1 MB | Weather forecasts, warnings, seasonal data (VPWW, VPZK, VPCK, etc.) |
| `extra_l.xml` | ~1.5 MB | Additional meteorological alerts (rarely used) |
| `eqvol_l.xml` | ~387 KB | **Earthquakes, volcanoes, tsunamis** (VXSE, VFVO, VTSE, VZSE, VZVO) |
| `other_l.xml` | ~245 KB | **Cherry blossoms, marine, phenology** (VGSK, VPCU, VPCY, VMCJ) |

Each feed is parsed into Atom entries with data type codes (VXSE53, VFVO51, etc.).

## Data Type Codes Reference

**Earthquakes** (`eqvol_l.xml`):
- VXSE51 - 震度速報 (Seismic Intensity Report)
- VXSE53 - 震源・震度に関する情報 (Earthquake & Seismic Intensity)
- VXSE56 - 地震の活動状況等に関する情報 (Earthquake Activity Status)
- VXSE60-62 - 震源・地震情報 (Seismic Observation)
- VXSE43/44 - 地震動速報 (Earthquake Early Warning)
- VTSE41 - 津波警報・注意報・予報 (Tsunami Warning/Advisory)
- VTSE51 - 津波情報 (Tsunami Information)
- VZSE40 - 地震・津波に関するお知らせ (Earthquake/Tsunami Notice)

**Volcanoes** (`eqvol_l.xml`):
- VFVO50 - 噴火警報・予報 (Eruption Warning/Forecast)
- VFVO51 - 火山の状況に関する解説情報 (Volcano Status Explanation)
- VFVO52 - 噴火に関する火山観測報 (Eruption Observation)
- VFVO53 - 降灰予報 (Volcanic Ash Forecast)
- VFVO56 - 噴火速報 (Eruption Flash Report)
- VFVO60 - 推定降灰範囲 (Estimated Plume Direction)
- VZVO40 - 火山に関するお知らせ (Volcano Notice)

**Marine** (`other_l.xml`):
- VPCU51 - 地方海上警報 (Regional Sea Alert)
- VPCY51 - 地方海上予報 (Regional Sea Forecast)
- VPZU50/51/54 - 全般海上警報 (General Marine Warning)
- VMCJ50 - 全般潮位情報 (General Tidal Information)
- VMCJ51 - 地方潮位情報 (Regional Tidal Information)

**Weather** (`regular_l.xml`):
- VPWW53-61 - 気象警報・注意報（定時） (Weather Warning/Advisory)
- VPOA50 - 記録的短時間大雨情報 (Record Short-term Heavy Rain)
- VPHW50/51 - 竜巻注意情報 (Tornado Watch)

**Hazards** (`regular_l.xml`):
- VXKO50-89 - 指定河川洪水予報 (Designated River Flood Forecast - 40 codes)
- VXWW50 - 土砂災害警戒情報 (Landslide Hazard Alert)

**Forecasts** (`regular_l.xml`):
- VPZK50 - 全般１か月予報・３か月予報 (General Seasonal Forecast)
- VPCK50 - 地方１か月予報・３か月予報 (Regional Seasonal Forecast)
- VPTI50/51/52 - 全般台風情報 (Typhoon Information)

**Phenology** (`other_l.xml`):
- VGSK50 - 季節観測 (Seasonal Observation)
- VGSK55 - 生物季節観測 (Phenological Observation - Cherry Blossom)
- VGSK60 - 特殊気象報 (Special Weather Report)

**References**:
- [JMA XML Format Portal](https://xml.kishou.go.jp/index.html) — Official index of all JMA XML data types, schemas, and feed documentation
- [JMA XML Format Specification v1.3](https://xml.kishou.go.jp/jmaxml_20260129_format_v1_3_hyo1_1.pdf) — Full specification document

## CI/CD

GitHub Actions workflow (`.github/workflows/daily-update.yml`):
- ⏰ Runs **every hour** at minute 0 UTC
- 🚀 Can be triggered manually via workflow dispatch
- 📦 **5 parallel matrix jobs** (one per category):
  1. Earthquakes & Seismic (6 datasets)
  2. Volcanoes & Eruptions (7 datasets)
  3. Tsunamis & Marine (5 datasets)
  4. Weather & Hazards (6 datasets)
  5. Forecasts & Observations (9 datasets)
- 📋 On failure: Creates GitHub issue with logs and download link

To set up CI/CD, add these secrets to your GitHub repository:

1. Go to your repo → **Settings** → **Secrets and variables** → **Actions**
2. Click **New repository secret** and add:

| Secret Name | Value |
|---|---|
| `KAGGLE_USERNAME` | Your Kaggle username (e.g., `hideos`) |
| `KAGGLE_API_TOKEN` | Your Kaggle API token (from Kaggle settings) |

After adding secrets, manually trigger the workflow or wait for the next hourly run.

## Logging & Metrics

Logs written to `logs/pipeline.log` and console (INFO level).

Example output:
```
2026-04-03 21:24:00,000 [INFO] data_pipeline: Processing dataset: japan-earthquake-and-seismic-information
2026-04-03 21:24:05,123 [INFO] jma_api_client.base: japan-earthquake-and-seismic-information: 46 rows fetched
2026-04-03 21:24:10,456 [INFO] kaggle_uploader: Downloaded 234 rows from hideos/japan-earthquake-and-seismic-information
2026-04-03 21:24:11,789 [INFO] kaggle_uploader: Merged: existing=234 + new=46 → combined=280 rows
2026-04-03 21:24:15,012 [INFO] kaggle_uploader: Upload successful (new version): hideos/japan-earthquake-and-seismic-information

Pipeline summary:
  Total rows fetched: 455
  Total pipeline time: 416.63 seconds
  Slowest datasets:
    japan-volcanic-ash-forecast: 83.66 seconds
    japan-regional-sea-alert: 16.37 seconds
```

Performance metrics tracked per dataset:
- Rows fetched
- Fetch time (seconds)
- Throughput (rows/sec)

## Troubleshooting

### Kaggle authentication fails
```bash
# Test Kaggle CLI credentials
kaggle datasets list
```
- Verify `.env` has correct `KAGGLE_USERNAME` and `KAGGLE_API_TOKEN`
- API key from account settings (not username/password)
- Ensure key has dataset read/write permissions

### "Dataset not found" error
- First run automatically creates dataset
- Verify username in `KAGGLE_USERNAME`
- Check you have write access to the dataset
- Dataset slug must start with `japan-` prefix

### Slow dataset parsing
- Some datasets (volcanic ash, sea data) are large XML files
- Consider increasing `MAX_ENTRIES` in dataset class to process fewer entries
- Hourly updates with 100 max entries per dataset is optimal

### Python import errors
- Ensure `pip install -r requirements.txt` completed
- Verify Python 3.11+ with `python --version`
- Check `jma_api_client/` files all exist (no missing modules)

## Project Structure

```
jma-kaggle-pipeline/
├── kaggle_uploader.py                  # Kaggle API integration
├── config.py                           # Dataset config & dynamic generation
├── logger.py                           # Logging setup
├── requirements.txt                    # Dependencies
├── .env.example                        # Environment template
├── README.md                           # This file
├── CLAUDE.md                           # Claude Code instructions
│
├── scripts/
│   ├── data_pipeline.py                # Main pipeline orchestrator
│   └── jma_datasets_overview.py        # Publish overview notebook to Kaggle
│
├── jma_api_client/                     # JMA API client (plugin architecture)
│   ├── __init__.py                     # Package exports
│   ├── base.py                         # JMADatasetBase + registry
│   ├── jma_earthquakes.py              # 7 earthquake/tsunami datasets
│   ├── jma_volcanoes.py                # 6 volcano/eruption datasets
│   ├── jma_sea.py                      # 2 regional sea datasets
│   ├── jma_marine.py                   # 3 marine/tidal datasets
│   ├── jma_weather.py                  # 3 weather warning datasets
│   ├── jma_hazards.py                  # 2 hazard (river/landslide) datasets
│   ├── jma_typhoon.py                  # 3 typhoon information datasets
│   ├── jma_forecasts.py                # 2 seasonal forecast datasets
│   ├── jma_phenology.py                # 3 phenology/seasonal datasets
│   ├── jma_notices.py                  # 2 informational notice datasets
│   ├── translate.py                    # Japanese→English translation
│   ├── utils.py                        # HTTP fetch, parsing, caching
│   └── temperature.py                  # (Legacy - AMeDAS)
│
├── docs/
│   ├── jma_datasets.md                 # JMA data type reference
│   └── kaggle_valid_tags.md            # Validated Kaggle keyword tags
│
├── tests/
│   └── test_pipeline.py                # Merge logic tests
│
├── .github/
│   ├── actions/python-setup/
│   │   └── action.yml                  # Reusable Python setup action
│   └── workflows/
│       ├── daily-update.yml            # Hourly data pipeline
│       └── publish-notebook.yml        # Weekly notebook publish
│
├── data/                               # Generated files (git-ignored)
│   └── raw/                            # Cached JMA feeds
│       ├── regular_l.xml
│       ├── extra_l.xml
│       ├── eqvol_l.xml
│       └── other_l.xml
│
└── logs/
    └── pipeline.log                    # Execution logs
```

## File Organization Notes

- **One module per domain** (e.g., `japan_earthquakes.py` has 8 related classes)
- **Not one-file-per-class** (more Pythonic and maintainable)
- **Related parsing logic stays together** for easy refactoring

## License

[MIT License - Add your license here]

## Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes with clear messages
4. Run `pytest` before pushing
5. Push to your fork and open a Pull Request

## Support

For issues, feature requests, or questions:
- Open a [GitHub Issue](https://github.com/hide-san/jma-kaggle-pipeline/issues)
- Check [CLAUDE.md](CLAUDE.md) for Claude Code integration details

---

**Made with ❤️ for the Kaggle and JMA data communities**

*Last updated: 2026-04-03*
