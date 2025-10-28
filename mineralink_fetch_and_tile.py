#!/usr/bin/env python3
"""
MineraLink Tiles Builder
Fetches GeoJSON data from ArcGIS REST endpoints, converts to vector tiles with Tippecanoe,
and deploys via GitHub Actions. Falls back to local sample data if sources fail.
"""

import os
import json
import time
import subprocess
import requests
import geopandas as gpd
from shapely.geometry import Point, Polygon, LineString

# ==============================
# CONFIGURATION
# ==============================

OUT_TILES_DIR = "tiles"
FALLBACK_DIR = "fallback_data"
TIPPECANOE = "tippecanoe"

# Maximum time to spend per dataset (seconds)
MAX_DATASET_SECONDS = 300
# Request timeout per chunk
REQUEST_TIMEOUT = 45
# Chunk grid divisions (5x5 = 25 queries per dataset)
GRID_DIVS = 5

# Datasets to fetch
DATASETS = [
    {
        "name": "WV_wells",
        "url": "https://tagis.dep.wv.gov/arcgis/rest/services/WVDEP_enterprise/oil_gas/MapServer/0/query",
        "bbox": [-82.8, 37.0, -77.7, 40.6],
    },
    {
        "name": "OH_parcels",
        "url": "https://geo.oit.ohio.gov/arcgis/rest/services/Statewide/Parcels/MapServer/0/query",
        "bbox": [-84.8, 38.3, -80.5, 42.0],
    },
    {
        "name": "TX_parcels",
        "url": "https://feature.geographic.texas.gov/arcgis/rest/services/Parcels/stratmap25_land_parcels_48/MapServer/0/query",
        "bbox": [-106.7, 25.7, -93.5, 36.6],
    },
]

# ==============================
# FUNCTIONS
# ==============================

def fetch_geojson(ds):
    """Fetch data from ArcGIS REST endpoint, return path to GeoJSON or None."""
    name, url = ds["name"], ds["url"]
    xmin, ymin, xmax, ymax = ds["bbox"]
    print(f"\n=== Fetching {name} ===")

    start = time.time()
    features = []

    for i in range(GRID_DIVS):
        for j in range(GRID_DIVS):
            if time.time() - start > MAX_DATASET_SECONDS:
                print(f"⏱️ Timeout for {name}")
                return None

            x0 = xmin + (xmax - xmin) / GRID_DIVS * i
            y0 = ymin + (ymax - ymin) / GRID_DIVS * j
            x1 = x0 + (xmax - xmin) / GRID_DIVS
            y1 = y0 + (ymax - ymin) / GRID_DIVS

            params = {
                "where": "1=1",
                "geometry": f"{x0},{y0},{x1},{y1}",
                "geometryType": "esriGeometryEnvelope",
                "inSR": "4326",
                "spatialRel": "esriSpatialRelIntersects",
                "outFields": "*",
                "returnGeometry": "true",
                "f": "json",
                "outSR": "4326",
            }

            try:
                r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
                r.raise_for_status()
                feats = r.json().get("features", [])
                if feats:
                    features += feats
                    print(f"  +{len(feats)} features ({i+1},{j+1})")
            except Exception as e:
                print(f"⚠️ Chunk {i+1},{j+1} failed: {e}")

    if not features:
        print(f"⚠️ No geometries fetched for {name}")
        return None

    # Build GeoDataFrame
    records = []
    for f in features:
        geom = f.get("geometry")
        props = f.get("attributes", {})
        try:
            if "x" in geom and "y" in geom:
                g = Point(geom["x"], geom["y"])
            elif "rings" in geom:
                g = Polygon(geom["rings"][0])
            elif "paths" in geom:
                g = LineString(geom["paths"][0])
            else:
                continue
            records.append({"geometry": g, **props})
        except Exception:
            continue

    if not records:
        print(f"⚠️ {name} parse failed")
        return None

    out_path = f"{name}.geojson"
    gdf = gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:4326")
    gdf.to_file(out_path, driver="GeoJSON")
    print(f"✅ Saved {name}.geojson ({len(gdf)} features)")
    return out_path


def build_tiles(name, geojson):
    """Convert GeoJSON to vector tiles using Tippecanoe."""
    if not geojson or not os.path.exists(geojson):
        print(f"⚠️ No GeoJSON for {name}, skipping tiling")
        return False

    os.makedirs(OUT_TILES_DIR, exist_ok=True)
    outdir = os.path.join(OUT_TILES_DIR, name)
    os.makedirs(outdir, exist_ok=True)

    cmd = [
        TIPPECANOE,
        "--output-to-directory", outdir,
        "--layer", name,
        "--force",
        "--minimum-zoom=4",
        "--maximum-zoom=14",
        geojson
    ]

    try:
        subprocess.run(cmd, check=True)
        print(f"✅ Built tiles for {name}")
        return True
    except Exception as e:
        print(f"⚠️ Tippecanoe failed for {name}: {e}")
        return False


# ==============================
# MAIN EXECUTION
# ==============================

def main():
    print("=== Starting MineraLink Tile Build ===")
    os.makedirs(OUT_TILES_DIR, exist_ok=True)
    built = []

    for ds in DATASETS:
        geojson = fetch_geojson(ds)
        if not geojson:
            # use fallback if available
            fallback = os.path.join(FALLBACK_DIR, f"{ds['name']}.geojson")
            if os.path.exists(fallback):
                print(f"🧩 Using fallback for {ds['name']}")
                geojson = fallback
            else:
                print(f"⚠️ No fallback for {ds['name']}, skipping.")
                continue

        if build_tiles(ds["name"], geojson):
            built.append(ds["name"])

    # If no tiles were built, use WV_wells fallback to avoid empty deploy
    if not built:
        print("⚠️ No datasets built successfully. Creating fallback WV_wells tile set...")
        fallback = os.path.join(FALLBACK_DIR, "WV_wells.geojson")
        if os.path.exists(fallback):
            build_tiles("WV_wells", fallback)
            built.append("WV_wells")
        else:
            print("❌ No fallback WV_wells.geojson found!")

    # Log summary
    if built:
        print(f"✅ Build complete. Tiles generated for: {built}")
    else:
        print("❌ No tiles generated at all!")

    print(f"Tiles directory: {OUT_TILES_DIR}")


if __name__ == "__main__":
    main()
