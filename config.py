import os
from dotenv import load_dotenv

load_dotenv()

KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")
KAGGLE_API_TOKEN = os.getenv("KAGGLE_API_TOKEN")

# JMA API base URLs
JMA_BASE_URL = "https://www.jma.go.jp/bosai"

# Dataset configurations
DATASETS = [
    {
        "name": "japan-earthquakes",
        "kaggle_dataset": f"{KAGGLE_USERNAME}/japan-earthquakes",
        "csv_filename": "earthquakes.csv",
        "merge_keys": ["event_id"],
        "description": "Earthquake data reported by JMA",
    },
    {
        "name": "japan-earthquakes-enhanced",
        "kaggle_dataset": f"{KAGGLE_USERNAME}/japan-earthquakes-enhanced",
        "csv_filename": "earthquakes_enhanced.csv",
        "merge_keys": ["event_id"],
        "description": "Enhanced earthquake data from JMA XML with hypocenter details and per-prefecture intensity",
    },
    {
        "name": "volcanic-ash-forecasts",
        "kaggle_dataset": f"{KAGGLE_USERNAME}/volcanic-ash-forecasts",
        "csv_filename": "volcanic_ash_forecasts.csv",
        "merge_keys": ["event_id"],
        "description": "Volcanic ash forecasts from JMA with 6-window time predictions and affected areas",
    },
]

# Retry settings for API calls
RETRY_ATTEMPTS = 3
RETRY_WAIT_SECONDS = 5

# Local data storage
DATA_DIR = "data"
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")

# Logging
LOG_FILE = "logs/pipeline.log"