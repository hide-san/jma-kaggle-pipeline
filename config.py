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
        "csv_filename": "japan_earthquakes.csv",
        "merge_keys": ["event_id"],
        "description": "Latest earthquake data from Japan Meteorological Agency",
        "keywords": ["jma", "japan", "earthquake", "seismic"],
        "subtitle": "Real-time earthquake updates from JMA",
    },
    {
        "name": "japan-earthquakes-enhanced",
        "kaggle_dataset": f"{KAGGLE_USERNAME}/japan-earthquakes-enhanced",
        "csv_filename": "japan_earthquakes_enhanced.csv",
        "merge_keys": ["event_id"],
        "description": "Enhanced earthquake data from JMA XML with hypocenter details and per-prefecture intensity",
        "keywords": ["jma", "japan", "earthquake", "seismic", "intensity"],
        "subtitle": "Detailed earthquake data with magnitude, intensity, and location",
    },
    {
        "name": "japan-volcanic-ash-forecasts",
        "kaggle_dataset": f"{KAGGLE_USERNAME}/japan-volcanic-ash-forecasts",
        "csv_filename": "japan_volcanic_ash_forecasts.csv",
        "merge_keys": ["event_id"],
        "description": "Volcanic ash forecasts from JMA with 6-window time predictions and affected areas",
        "keywords": ["jma", "japan", "volcano", "ash", "forecast"],
        "subtitle": "6-hour window volcanic ash forecasts from Japanese volcanoes",
    },
    {
        "name": "japan-cherry-blossom-observations",
        "kaggle_dataset": f"{KAGGLE_USERNAME}/japan-cherry-blossom-observations",
        "csv_filename": "japan_cherry_blossom_observations.csv",
        "merge_keys": ["event_id"],
        "description": "Cherry blossom phenophase observations from JMA stations with locations",
        "keywords": ["jma", "japan", "cherry-blossom", "phenology", "observation"],
        "subtitle": "Annual cherry blossom blooming observations from JMA stations",
    },
    {
        "name": "japan-volcano-status",
        "kaggle_dataset": f"{KAGGLE_USERNAME}/japan-volcano-status",
        "csv_filename": "japan_volcano_status.csv",
        "merge_keys": ["event_id"],
        "description": "Volcano status reports from JMA with alert levels and activity summaries",
        "keywords": ["jma", "japan", "volcano", "status", "alert"],
        "subtitle": "Real-time volcano status and alert levels",
    },
    {
        "name": "japan-sea-warnings",
        "kaggle_dataset": f"{KAGGLE_USERNAME}/japan-sea-warnings",
        "csv_filename": "japan_sea_warnings.csv",
        "merge_keys": ["event_id"],
        "description": "Regional sea warnings from JMA with warning types and affected areas",
        "keywords": ["jma", "japan", "sea", "warning", "maritime"],
        "subtitle": "Regional maritime warnings including tsunami and high-wave alerts",
    },
    {
        "name": "japan-sea-forecasts",
        "kaggle_dataset": f"{KAGGLE_USERNAME}/japan-sea-forecasts",
        "csv_filename": "japan_sea_forecasts.csv",
        "merge_keys": ["event_id"],
        "description": "Regional sea forecasts from JMA with forecast types and affected areas",
        "keywords": ["jma", "japan", "sea", "forecast", "maritime"],
        "subtitle": "Regional sea forecasts including wave heights and conditions",
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