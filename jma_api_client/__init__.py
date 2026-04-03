"""JMA (Japan Meteorological Agency) API client.

Endpoints:
- Earthquake list (JSON): https://www.jma.go.jp/bosai/quake/data/list.json
  Simple list of recent earthquakes with basic info (magnitude, intensity, location)

- JMA Data Feeds (XML Atom):
  - regular_l.xml: Regular information (3.1MB, weather, forecasts, etc.)
  - extra_l.xml: Extra/additional information (1.5MB)
  - eqvol_l.xml: Earthquake & Volcano information (387KB) - Primary source for earthquake data
  - other_l.xml: Other information (245KB)

  Each feed contains Atom entries with links to detailed meteorological and seismic data files.
"""

# Plugin registry infrastructure
from .base import DATASET_REGISTRY, JMADatasetBase, register_dataset

# Dataset classes (new plugin architecture)
from .japan_earthquakes import (
    EarthquakeIntensityInfo,
    SeismicIntensityReport,
    TsunamiWarning,
    EarthquakeEarlyWarning,
    TsunamiInfo,
)
from .japan_volcanoes import (
    VolcanoStatusExplanation,
    VolcanicAshForecast,
    EruptionWarning,
    EruptionFlashReport,
    EruptionObservation,
    EstimatedPlumeDirection,
)
from .japan_sea import RegionalSeaAlert, RegionalSeaForecast
from .japan_cherry_blossom import (
    PhenologicalObservation,
    SeasonalObservation,
    SpecialWeatherReport,
)
from .japan_weather import (
    WeatherWarning,
    HeavyRainWarning,
    TornadoWatchInfo,
)
from .japan_marine import (
    GeneralMarineWarning,
    RegionalTidalInfo,
    GeneralTidalInfo,
)

# Legacy functions (backwards compatibility)
from .japan_earthquakes import fetch_earthquake_data, fetch_earthquakes_enhanced
from .japan_volcanoes import fetch_volcanic_ash_forecasts, fetch_volcano_status
from .japan_sea import fetch_sea_warnings, fetch_sea_forecasts
from .japan_cherry_blossom import fetch_cherry_blossom_observations
from .temperature import fetch_temperature_data

__all__ = [
    # Registry & base infrastructure
    "DATASET_REGISTRY",
    "JMADatasetBase",
    "register_dataset",
    # Dataset classes (earthquakes & seismic)
    "EarthquakeIntensityInfo",
    "SeismicIntensityReport",
    "TsunamiWarning",
    "EarthquakeEarlyWarning",
    "TsunamiInfo",
    # Dataset classes (volcanoes)
    "VolcanoStatusExplanation",
    "VolcanicAshForecast",
    "EruptionWarning",
    "EruptionFlashReport",
    "EruptionObservation",
    "EstimatedPlumeDirection",
    # Dataset classes (sea & seasonal)
    "RegionalSeaAlert",
    "RegionalSeaForecast",
    "PhenologicalObservation",
    "SeasonalObservation",
    "SpecialWeatherReport",
    # Dataset classes (weather)
    "WeatherWarning",
    "HeavyRainWarning",
    "TornadoWatchInfo",
    # Dataset classes (marine)
    "GeneralMarineWarning",
    "RegionalTidalInfo",
    "GeneralTidalInfo",
    # Legacy functions
    "fetch_earthquake_data",
    "fetch_earthquakes_enhanced",
    "fetch_volcanic_ash_forecasts",
    "fetch_volcano_status",
    "fetch_sea_warnings",
    "fetch_sea_forecasts",
    "fetch_cherry_blossom_observations",
    "fetch_temperature_data",
]
