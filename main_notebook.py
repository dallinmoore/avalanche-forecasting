# -*- coding: utf-8 -*-
"""main_notebook_optimized.py

End‑to‑end pipeline for collecting SNOTEL metadata & daily time‑series,
assigning each station to one or more Utah avalanche forecast regions
(with elevation bands), and writing a tidy CSV ready for modelling.

Major speed‑ups vs the original notebook
---------------------------------------
1. **Spatial look‑ups**
   * Build a Shapely **STRtree** (R‑tree) once, so every station only
     tests a handful of candidate polygons (instead of every region).
   * Use **prepared polygons** – `prep()` – so `contains` calls are
     ~3‑10× faster.
   * Fix coordinate order: `(x, y) = (lon, lat)`.
2. **Data structures**
   * `region_stations` is now *dict‑of‑dicts* (`region → stationId → info`)
     giving **O(1)** random access and no duplicates.
3. **Vectorised elevation bands**
   * Elevation level assignment is done once during the region pass.
4. **Threaded API fetch** – unchanged (network bound), but wrapped so the
   rest of the code can be imported as a module without running it.
"""

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Dict, List

import pandas as pd
import numpy as np
from shapely.geometry import Point, Polygon
from shapely.strtree import STRtree
from shapely.prepared import prep
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon as MplPolygon
from matplotlib.collections import PatchCollection
import matplotlib.colors as mcolors

# ---------------------------------------------------------------------------
# 0.  CONSTANTS
# ---------------------------------------------------------------------------
BASE_URL = "https://wcc.sc.egov.usda.gov/awdbRestApi/services/v1/"

REGION_BOUNDARIES: Dict[str, List[List[float]]] = {
    "Logan": [[41.2, -111.9], [42.0, -111.9], [42.0, -111.1], [41.2, -111.1]],
    "Ogden": [[40.8, -112.0], [41.2, -112.0], [41.2, -111.1], [40.8, -111.1]],
    "Salt Lake": [[40.4, -112.0], [40.8, -112.0], [40.8, -111.1], [40.4, -111.1]],
    "Uintas": [[40.3, -111.1], [41.0, -111.1], [41.0, -109.4], [40.3, -109.4]],
    "Provo": [[39.8, -112.0], [40.4, -112.0], [40.4, -111.1], [39.8, -111.1]],
    "Skyline": [[38.9, -111.8], [39.8, -111.8], [39.8, -111.1], [38.9, -111.1]],
    "Moab": [[38.0, -109.8], [39.0, -109.8], [39.0, -108.9], [38.0, -108.9]],
    "Abajos": [[37.5, -109.9], [37.9, -109.9], [37.9, -109.1], [37.5, -109.1]],
    "Southwest": [[37.0, -114.0], [38.5, -114.0], [38.5, -112.5], [37.0, -112.5]],
}

# Elevation bands (abridged to the six modelling regions)
REGION_ELEVATIONS = {
    "Logan":    [(0, 7000), (7001, 8500), (8501, 20000)],
    "Ogden":    [(0, 7000), (7001, 8500), (8501, 20000)],
    "Uintas":   [(0, 9500), (9501, 10000), (10001, 20000)],
    "Salt Lake":[(0, 8000), (8001, 9500), (9501, 20000)],
    "Provo":    [(0, 8000), (8001, 9500), (9501, 20000)],
    "Skyline":  [(0, 8000), (8001, 9500), (9501, 20000)],
}

SNOTEL_ELEMENTS = {
    "Snow_Depth": "SNWD",
    "SWE": "WTEQ",
    "Precipitation_Increment": "PRCP",
    "Snow_Density": "SNDN",
    "Avg_Temp": "TAVG",
    "Max_Temp": "TMAX",
    "Min_Temp": "TMIN",
}

# ---------------------------------------------------------------------------
# 1.  HELPERS – SPATIAL INDEX
# ---------------------------------------------------------------------------

def _build_region_index(boundaries: Dict[str, List[List[float]]], min_threshold: float = 0.4):
    """Return polygons, prepared geoms, STRtree and a mapping id→region."""
    polys, region_by_id, prepared = [], {}, {}
    for region, ll in boundaries.items():
        # Shapely expects (x, y) = (lon, lat)
        coords = [(lon, lat) for lat, lon in ll]
        poly = Polygon(coords)
        polys.append(poly)
        pid = id(poly)
        region_by_id[pid] = region
        prepared[pid] = prep(poly)
    return polys, region_by_id, prepared, STRtree(polys), min_threshold

_POLYS, _REGION_BY_ID, _PREPARED, _TREE, _THRESH = _build_region_index(REGION_BOUNDARIES)


def regions_for_point(lat: float, lon: float) -> List[str]:
    """Return all regions whose polygon contains (or is near) the point."""
    pt = Point(lon, lat)  # (x, y)
    regions = []
    for poly in _TREE.query(pt):  # bbox candidates
        pid = id(poly)
        reg = _REGION_BY_ID[pid]
        if _PREPARED[pid].contains(pt):
            regions.append(reg)
        elif pt.distance(poly.exterior) <= _THRESH:
            regions.append(reg)
    return regions

# ---------------------------------------------------------------------------
# 2.  METADATA – STATIONS
# ---------------------------------------------------------------------------

def fetch_snotel_metadata(state: str = "UT") -> pd.DataFrame:
    """Return a DataFrame of active SNOTEL stations for *state*"""
    url = BASE_URL + "stations"
    params = {
        "stationTriplets": f"*: {state}:SNTL".replace(" ", ""),
        "returnForecastPointMetadata": "false",
        "returnReservoirMetadata": "false",
        "returnStationElements": "false",
        "activeOnly": "true",
        "durations": "HOURLY",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    keys = [
        "stationId", "stateCode", "networkCode", "name", "countyName",
        "elevation", "latitude", "longitude",
    ]
    return pd.DataFrame([{k: s.get(k, "") for k in keys} for s in r.json()])


def assign_regions(df: pd.DataFrame):
    """Return (region_stations, station_to_regions).

    *region_stations*: {region → {stationId → info + elevation_level}}
    *station_to_regions*: {stationId → [regions...]}
    """
    region_stations: Dict[str, Dict[str, dict]] = {r: {} for r in REGION_BOUNDARIES}
    station_to_regions: Dict[str, List[str]] = {}

    for row in df.itertuples(index=False):
        regs = regions_for_point(row.latitude, row.longitude)
        if not regs:
            continue
        for reg in regs:
            band = REGION_ELEVATIONS.get(reg)
            if band:
                lvl = 1 if row.elevation <= band[0][1] else 2 if row.elevation <= band[1][1] else 3
            else:
                lvl = 0
            region_stations[reg][row.stationId] = {
                "stationId": row.stationId,
                "name": row.name,
                "elevation": row.elevation,
                "latitude": row.latitude,
                "longitude": row.longitude,
                "elevation_level": lvl,
            }
        station_to_regions[row.stationId] = regs
    return region_stations, station_to_regions

# ---------------------------------------------------------------------------
# 3.  DAILY TIME‑SERIES
# ---------------------------------------------------------------------------

def fetch_snotel_timeseries(station_ids: List[str], begin_date: str, end_date: str = date.today().strftime("%Y-%m-%d"), max_workers: int = 12) -> pd.DataFrame:
    def _fetch_triplet(triplet: str):
        url = BASE_URL + "data"
        params = {
            "stationTriplets": triplet,
            "beginDate": begin_date,
            "endDate": end_date,
            "elements": ",".join(SNOTEL_ELEMENTS.values()),
            "duration": "DAILY",
        }
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def _process_station(st_id: str):
        triplet = f"{st_id}:UT:SNTL"
        results = []
        for site in _fetch_triplet(triplet):
            for elem in site.get("data", []):
                code = elem["stationElement"]["elementCode"]
                df_name = {v: k for k, v in SNOTEL_ELEMENTS.items()}[code]
                for v in elem.get("values", []):
                    rec = {
                        "Date": v["date"],
                        "stationId": st_id,
                        df_name: v.get("value"),
                    }
                    if "average" in v:
                        rec[f"{df_name}_avg"] = v.get("average")
                    results.append(rec)
        return results

    all_rows: List[dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for fut in as_completed({ex.submit(_process_station, sid): sid for sid in station_ids}):
            try:
                all_rows.extend(fut.result())
            except Exception as e:
                sid = fut.exception()
                print(f"⚠️  {sid}: {e}")
    return pd.DataFrame(all_rows)

# ---------------------------------------------------------------------------
# 4.  TIDY DATASET
# ---------------------------------------------------------------------------

def build_dataset(start_date: str = "2013-12-13") -> pd.DataFrame:
    stations_df = fetch_snotel_metadata()
    region_stations, station_to_regions = assign_regions(stations_df)

    unique_ids = list(station_to_regions.keys())
    ts_df = fetch_snotel_timeseries(unique_ids, start_date)

    # Join metadata → vectorised merge is cheap
    ds = ts_df.merge(stations_df, on="stationId", how="left")

    # Expand rows for multi‑region stations
    rows = []
    for r in ds.itertuples(index=False):
        for reg in station_to_regions.get(r.stationId, ["Unknown"]):
            base = r._asdict()
            base["Region"] = reg
            base["elevation_level"] = (
                region_stations.get(reg, {})
                .get(r.stationId, {})
                .get("elevation_level", "Unknown")
            )
            rows.append(base)
    cols = [
        "Region", "stationId", "elevation_level", "Date", "name", "stateCode",
        "networkCode", "countyName", "elevation", "latitude", "longitude",
        *SNOTEL_ELEMENTS.keys(),
    ]
    return pd.DataFrame(rows)[cols]

# ---------------------------------------------------------------------------
# 5.  VISUALIZATION
# ---------------------------------------------------------------------------

def plot_regions(stations_df: pd.DataFrame, region_stations: Dict[str, Dict[str, dict]]):
    colors = list(mcolors.TABLEAU_COLORS)
    fig, ax = plt.subplots(figsize=(10, 8))

    # Utah outline (approx)
    utah = np.array([
        [42.001, -114.053], [42.001, -111.046], [41.000, -111.046],
        [41.000, -109.050], [37.000, -109.050], [37.000, -114.050],
        [42.001, -114.053],
    ])[:, [1, 0]]
    ax.add_patch(MplPolygon(utah, closed=True, fill=False, edgecolor="black", lw=2))

    patches, centroids = [], {}
    for i, (reg, ll) in enumerate(REGION_BOUNDARIES.items()):
        coords = np.array(ll)[:, [1, 0]]
        patches.append(MplPolygon(coords, closed=True))
        centroids[reg] = (coords[:, 0].mean(), coords[:, 1].mean())
    p = PatchCollection(patches, alpha=0.4)
    p.set_array(np.arange(len(patches)))
    ax.add_collection(p)
    plt.colorbar(p)

    for reg, (x, y) in centroids.items():
        ax.text(x, y, reg, ha="center", va="center", fontsize=9)
        ax.text(x, y - 0.1, f"({len(region_stations[reg])} stations)", ha="center", va="center", fontsize=7)

    ax.scatter(stations_df.longitude, stations_df.latitude, c="black", s=15, alpha=0.7)
    ax.set_xlim(-114.5, -109.0)
    ax.set_ylim(37.0, 42.0)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title("Utah Avalanche Regions & SNOTEL Stations")
    ax.grid(ls="--", alpha=0.6)
    fig.tight_layout()
    plt.show()

# ---------------------------------------------------------------------------
# 6.  MAIN (run only if executed, not on import)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("Fetching and building dataset - this may take a few minutes the first time…")
    ds = build_dataset()
    out_path = Path("snotel_dataset.csv")
    ds.to_csv(out_path, index=False)
    print(f"✓ Saved {len(ds):,} rows → {out_path}")

    # Visual check
    stations_df = fetch_snotel_metadata()
    region_stations, _ = assign_regions(stations_df)
    plot_regions(stations_df, region_stations)
