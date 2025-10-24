#!/usr/bin/env python3
# mineralink_fetch_and_tile.py
#
# Fetches ArcGIS well data for WV, OH, PA, and TX, reprojects to EPSG:4326,
# merges into one GeoJSON, builds Tippecanoe vector tiles into /tiles/,
# ready for deployment via gh-pages.

import os
import json
import shutil
import subprocess
import tempfile
import requests
import geopandas as gpd
from pathlib import Path

# -----------------------------
# CONFIG
# -----------------------------

DATASETS = [
    {
        "name": "WV_wells",
        "url": "https://<arcgis-server>/WV/WellData/FeatureServer/0/query",
    },
    {
        "name": "OH_wells",
        "url": "https://<arcgis-server>/OH/WellData/FeatureServer/0/query",
    },
    {
        "name": "PA_wells",
        "url": "https://<arcgis-server>/PA/WellData/FeatureServer/0/query",
    },
    {
        "name": "TX_wells",
        "url": "https://<arcgis-server>/TX/WellData/FeatureServer/0/query",
    },
]

OUT_TILES_DIR = Path("tiles")
MERGED_GEOJSON = "all_states_wells_4326.geojson"

TIPPECANOE_CMD = "tippecanoe"
TIPPECANOE_MINZOOM = 4
TIPPECANOE_MAXZOOM = 14
LAYER_NAME = "wells"

# -----------------------------
# FUNCTIONS
# -----------------------------

def fetch_geojson(dataset):
    """Fetch full dataset as GeoJSON."""
    name = dataset["name"]
    url = dataset["url"]
    params = {
        "where": "1=1",
        "outFields": "*",
        "f": "geojson",
        "outSR": "4326",
    }

    print(f"Fetching {name} from {url}")
    resp = requests.get(url, params=params, timeout=180)
    resp.raise_for_status()
    geo = resp.json()
    file_name = f"{name}.geojson"
    with open(file_name, "w", encoding="utf-8") as f:
        json.dump(geo, f)
    print(f"Saved {file_name}")
    return file_name


def reproject_to_4326(input_file):
    """Force reprojection to EPSG:4326."""
    print(f"Reprojecting {input_file} to EPSG:4326")
    gdf = gpd.read_file(input_file)
    gdf = gdf.to_crs(epsg=4326)
    out_file = input_file.replace(".geojson", "_4326.geojson")
    gdf.to_file(out_file, driver="GeoJSON")
    print(f"Wrote {out_file}")
    return out_file


def merge_geojsons(file_list, merged_output):
    """Combine all GeoJSONs into one unified file."""
    print("Merging datasets into one GeoJSON...")
    dfs = [gpd.read_file(f) for f in file_list]
    merged = gpd.GeoDataFrame(pd.concat(dfs, ignore_index=True), crs="EPSG:4326")
    merged.to_file(merged_output, driver="GeoJSON")
    print(f"Created {merged_output}")
    return merged_output


def build_tiles(geojson_file, out_dir):
    """Run Tippecanoe to generate .pbf tiles."""
    if out_dir.exists():
        print(f"Cleaning {out_dir}")
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        TIPPECANOE_CMD,
        "--output-to-directory", str(out_dir),
        "--layer", LAYER_NAME,
        "--minimum-zoom", str(TIPPECANOE_MINZOOM),
        "--maximum-zoom", str(TIPPECANOE_MAXZOOM),
        "--force",
        str(geojson_file),
    ]
    print("Running Tippecanoe:", " ".join(cmd))
    subprocess.check_call(cmd)
    print(f"Tiles written to {out_dir}")


def main():
    print("=== Starting Mineralink Tile Builder ===")

    fetched = []
    reprojected = []

    for ds in DATASETS:
        f = fetch_geojson(ds)
        fetched.append(f)
        r = reproject_to_4326(f)
        reprojected.append(r)

    merged = merge_geojsons(reprojected, MERGED_GEOJSON)
    build_tiles(merged, OUT_TILES_DIR)

    # Cleanup intermediate files
    for f in fetched + reprojected:
        try:
            os.remove(f)
        except OSError:
            pass

    print("All done! Tiles ready in /tiles/")


if __name__ == "__main__":
    import pandas as pd  # moved import here to avoid early overhead
    main()
