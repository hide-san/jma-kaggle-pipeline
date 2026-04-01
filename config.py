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
        "name": "cherry-blossom-observations",
        "kaggle_dataset": f"{KAGGLE_USERNAME}/cherry-blossom-observations",
        "csv_filename": "cherry_blossom.csv",
        "merge_keys": ["year", "station_no"],
        "description": "Cherry blossom blooming observations by JMA",
    },
    {
        "name": "japan-city-temperatures",
        "kaggle_dataset": f"{KAGGLE_USERNAME}/japan-city-temperatures",
        "csv_filename": "temperatures.csv",
        "merge_keys": ["datetime", "station_no"],
        "description": "Daily temperature observations at major cities in Japan by JMA",
    },
    {
        "name": "japan-earthquakes",
        "kaggle_dataset": f"{KAGGLE_USERNAME}/japan-earthquakes",
        "csv_filename": "earthquakes.csv",
        "merge_keys": ["event_id"],
        "description": "Earthquake data reported by JMA",
    },
]

# Retry settings for API calls
RETRY_ATTEMPTS = 3
RETRY_WAIT_SECONDS = 5

# Logging
LOG_FILE = "logs/pipeline.log"