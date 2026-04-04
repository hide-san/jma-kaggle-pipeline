# Kaggle Valid Tags Reference

Tags were validated by uploading a test dataset with all candidate keywords and observing
which ones Kaggle accepted vs. silently rejected. The CLI reports invalid tags explicitly:

> "The following are not valid tags and could not be added to the dataset: [...]"

Last verified: 2026-04-04

---

## Valid Tags Relevant to JMA Datasets

These are the Kaggle-recognized tags that apply to JMA meteorological/seismic data:

| Tag | Relevant to |
|-----|-------------|
| `japan` | All datasets |
| `asia` | All datasets |
| `east-asia` | All datasets |
| `earthquake` | Earthquake/seismic datasets |
| `natural-disaster` | Earthquakes, tsunami, flood, landslide, volcano |
| `disaster` | Earthquakes, tsunami, flood, landslide, volcano |
| `weather` | Weather warnings, typhoon, seasonal forecasts |
| `atmospheric-science` | Weather, typhoon, seasonal forecasts |
| `climate-change` | Seasonal forecasts, phenology, weather |
| `temperature` | Seasonal forecasts |
| `wind` | Marine, sea, typhoon |
| `ocean` | Marine, sea, tidal |
| `sea` | Marine, sea warnings |
| `river` | River flood forecasts |
| `water` | River flood, tidal |
| `environment` | General nature/ecology datasets |
| `plants` | Phenology / cherry blossom |
| `aviation` | Volcanic ash forecast, plume direction |
| `public-safety` | All hazard/warning datasets |
| `emergency-response` | All hazard/warning datasets |
| `government` | All datasets (JMA is a government agency) |
| `gis` | Datasets with location data |
| `geography` | Datasets with location data |
| `remote-sensing` | Observation datasets |
| `alerts` | Warning/alert datasets |
| `events` | Event-based datasets (seismic events, eruptions) |

---

## Invalid Tags Tested (Rejected by Kaggle)

These look relevant but are **not** in Kaggle's tag catalog:

`activity`, `advisory`, `alert`, `alert-level`, `anomaly`, `ash`, `bloom`,
`cherry-blossom`, `clarification`, `climate`, `coast`, `coastal`, `csv`,
`cyclone`, `data-format`, `detailed`, `direction`, `dispersion`, `district`,
`earth-and-nature`, `earth-science`, `ecology`, `emergency`, `eruption`,
`extended`, `flash-report`, `flood`, `flooding`, `forecast`, `geophysics`,
`geospatial`, `hazard`, `heavy`, `high-wave`, `hurricane`, `hydrology`,
`impacts`, `information`, `intensity`, `jma`, `landslide`, `marine`,
`maritime`, `meteorology`, `monitoring`, `monthly`, `natural-disasters`,
`notice`, `observation`, `oceanography`, `phenology`, `phenomenon`, `plume`,
`position`, `precipitation`, `predictions`, `rain`, `rainfall`, `regional`,
`seismology`, `severe-weather`, `shipping`, `snow`, `special-report`,
`standardized`, `storm`, `temperature` *(see note)*, `tidal`, `tide`,
`time-series`, `tornado`, `trees`, `tropical`, `tsunami`, `typhoon`,
`unusual`, `volcano`, `volcanology`, `warning`, `watch`, `wave`,
`wave-height`, `wildfire`

> **Notable absences:** `tsunami`, `typhoon`, `volcano`, `flood`, `seismology`,
> `meteorology`, `geospatial` are all rejected — use `natural-disaster` + `disaster`
> instead for hazard datasets.

---

## Tag Assignment by Dataset Category

| Dataset category | Recommended tags |
|-----------------|-----------------|
| Earthquake / seismic | `japan`, `earthquake`, `natural-disaster`, `disaster`, `public-safety`, `emergency-response`, `east-asia`, `gis` |
| Tsunami | `japan`, `natural-disaster`, `disaster`, `public-safety`, `ocean`, `sea`, `east-asia`, `emergency-response` |
| Volcano | `japan`, `natural-disaster`, `disaster`, `public-safety`, `environment`, `east-asia`, `aviation` |
| Volcanic ash forecast | `japan`, `natural-disaster`, `aviation`, `environment`, `east-asia` |
| Weather warnings | `japan`, `weather`, `atmospheric-science`, `public-safety`, `environment`, `asia` |
| Typhoon | `japan`, `weather`, `disaster`, `natural-disaster`, `east-asia`, `public-safety`, `wind` |
| Marine / sea | `japan`, `ocean`, `sea`, `weather`, `environment`, `east-asia`, `wind` |
| River flood | `japan`, `natural-disaster`, `disaster`, `river`, `water`, `public-safety`, `emergency-response` |
| Landslide | `japan`, `natural-disaster`, `disaster`, `public-safety`, `emergency-response`, `environment` |
| Seasonal forecast | `japan`, `weather`, `temperature`, `atmospheric-science`, `climate-change`, `asia` |
| Phenology / cherry blossom | `japan`, `plants`, `environment`, `climate-change`, `asia`, `geography` |
| Special weather / notices | `japan`, `weather`, `government`, `events`, `alerts`, `asia` |
