import os

from dotenv import load_dotenv

load_dotenv()

KAGGLE_USERNAME = os.getenv("KAGGLE_USERNAME")
KAGGLE_API_TOKEN = os.getenv("KAGGLE_API_TOKEN")

# JMA API base URLs
JMA_BASE_URL = "https://www.jma.go.jp/bosai"

# Local data storage
DATA_DIR = "data"
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")

# Logging (define before importing jma_api_client to avoid circular imports)
LOG_FILE = "logs/pipeline.log"

# Retry settings for API calls
RETRY_ATTEMPTS = 3
RETRY_WAIT_SECONDS = 5

# Import dataset registry and build DATASETS dynamically
from jma_api_client.base import DATASET_REGISTRY

DATASETS = []
for dataset_name in sorted(DATASET_REGISTRY.keys()):
    dataset_cls = DATASET_REGISTRY[dataset_name]
    config_dict = dataset_cls.to_config()
    # Add Kaggle username to kaggle_dataset (may be None when credentials are not needed)
    config_dict["kaggle_dataset"] = config_dict["kaggle_dataset"].replace(
        "{KAGGLE_USERNAME}", KAGGLE_USERNAME or ""
    )
    DATASETS.append(config_dict)
