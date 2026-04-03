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

from .japan_earthquakes import fetch_earthquake_data, fetch_earthquakes_enhanced
from .japan_volcanoes import fetch_volcanic_ash_forecasts, fetch_volcano_status
from .japan_sea import fetch_sea_warnings, fetch_sea_forecasts
from .japan_cherry_blossom import fetch_cherry_blossom_observations
from .temperature import fetch_temperature_data

__all__ = [
    "fetch_earthquake_data",
    "fetch_earthquakes_enhanced",
    "fetch_volcanic_ash_forecasts",
    "fetch_volcano_status",
    "fetch_sea_warnings",
    "fetch_sea_forecasts",
    "fetch_cherry_blossom_observations",
    "fetch_temperature_data",
]
