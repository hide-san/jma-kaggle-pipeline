"""
Temperature data fetching from JMA APIs.

NOTE: This data source has been discontinued by JMA.
The AMeDAS API endpoint no longer provides data.

Official Resource: (Discontinued - AMeDAS Temperature)
Source: jma.go.jp/bosai/amedas (deprecated)
"""

import pandas as pd
from logger import get_logger

log = get_logger(__name__)


def fetch_temperature_data() -> pd.DataFrame:
    """Fetch latest AMeDAS surface temperature snapshot for all stations.

    NOTE: The AMeDAS map endpoint has been discontinued by JMA.
    The /bosai/amedas/data/map/{datetime}.json endpoint returns 404.
    This dataset is no longer available from JMA.
    """
    log.info("Fetching temperature data from AMeDAS")
    log.warning("AMeDAS temperature endpoint has been discontinued by JMA - no data available")
    # Return empty DataFrame to allow pipeline to continue gracefully
    return pd.DataFrame()
