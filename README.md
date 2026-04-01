# JMA-Kaggle Pipeline

Automated data pipeline that fetches meteorological and seismic data from the **Japan Meteorological Agency (JMA)**, merges with existing Kaggle datasets, and publishes updates daily.

## Features

- **Three Datasets**: Cherry blossom observations, city temperatures (AMeDAS), and earthquake data
- **Automated Daily Updates**: Runs at 09:00 JST via GitHub Actions
- **Intelligent Merging**: Deduplicates on configurable keys; newer data takes precedence
- **Error Handling**: Retry logic for API calls and automatic issue creation on pipeline failure
- **Logging**: Full pipeline execution logs for debugging and monitoring

## Data Sources

| Dataset | Source | Update Frequency |
|---------|--------|------------------|
| **Cherry Blossom Observations** | JMA Sakura Data | Annual (spring) |
| **City Temperatures** | AMeDAS Network | Daily (latest snapshot) |
| **Earthquakes** | JMA Seismic Data | Real-time |

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

- **Cherry Blossom**: CSV from `data.jma.go.jp` (Shift-JIS encoded)
- **Temperature**: JSON snapshots from AMeDAS network (`jma.go.jp/bosai/amedas`)
- **Earthquakes**: JSON list from JMA seismic database

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
├── data_pipeline.py          # Main orchestrator
├── jma_api_client.py         # JMA API client
├── kaggle_uploader.py        # Kaggle integration
├── config.py                 # Configuration
├── logger.py                 # Logging setup
├── requirements.txt          # Python dependencies
├── .env.example              # Environment template
├── tests/
│   └── test_pipeline.py      # Merge logic tests
└── .github/workflows/
    └── daily-update.yml      # GitHub Actions workflow
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
