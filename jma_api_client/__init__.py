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
# Legacy functions (backwards compatibility)
from .jma_earthquakes import (
    EarthquakeActivityInfo,
    EarthquakeEarlyWarning,
    EarthquakeIntensityInfo,
    SeismicIntensityReport,
    SeismicObservationInfo,
    TsunamiInfo,
    TsunamiWarning,
    fetch_earthquakes_enhanced,
)
from .jma_forecasts import (
    GeneralSeasonalForecast,
    RegionalSeasonalForecast,
)
from .jma_hazards import (
    LandslideHazardAlert,
    RiverFloodForecast,
)
from .jma_marine import (
    GeneralMarineWarning,
    GeneralTidalInfo,
    RegionalTidalInfo,
)
from .jma_notices import (
    EarthquakeTsunamiNotice,
    VolcanoNotice,
)
from .jma_phenology import (
    PhenologicalObservation,
    SeasonalObservation,
    SpecialWeatherReport,
    fetch_cherry_blossom_observations,
)
from .jma_phenology_archive import PhenologicalObservationArchive
from .jma_sea import RegionalSeaAlert, RegionalSeaForecast, fetch_sea_forecasts, fetch_sea_warnings
from .jma_typhoon import (
    TyphoonInfoDetailed,
    TyphoonInfoGeneral,
    TyphoonInfoStandardized,
)
from .jma_volcanoes import (
    EruptionFlashReport,
    EruptionObservation,
    EruptionWarning,
    EstimatedPlumeDirection,
    VolcanicAshForecast,
    VolcanoStatusExplanation,
    fetch_volcanic_ash_forecasts,
    fetch_volcano_status,
)
from .jma_weather import (
    HeavyRainWarning,
    TornadoWatchInfo,
    WeatherWarning,
)
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
    "EarthquakeActivityInfo",
    "SeismicObservationInfo",
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
    "PhenologicalObservationArchive",
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
    # Dataset classes (typhoon)
    "TyphoonInfoGeneral",
    "TyphoonInfoStandardized",
    "TyphoonInfoDetailed",
    # Dataset classes (hazards)
    "RiverFloodForecast",
    "LandslideHazardAlert",
    # Dataset classes (forecasts)
    "GeneralSeasonalForecast",
    "RegionalSeasonalForecast",
    # Dataset classes (notices)
    "EarthquakeTsunamiNotice",
    "VolcanoNotice",
    # Legacy functions
    "fetch_earthquakes_enhanced",
    "fetch_volcanic_ash_forecasts",
    "fetch_volcano_status",
    "fetch_sea_warnings",
    "fetch_sea_forecasts",
    "fetch_cherry_blossom_observations",
    "fetch_temperature_data",
]
