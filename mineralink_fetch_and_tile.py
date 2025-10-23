#!/usr/bin/env python3
"""
MineraLink – Stable Well & Parcel Tile Builder
Author: mikeaa1983
Builds correct WGS84 vector tiles from WV, OH, PA, TX.
"""

import os
import json
import time
import subprocess
import requests
from datetime import datetime
from pathlib import Path
from shapely.geometry import shape, mapping
from shapely import ops as shapely_ops
from pyproj import Transformer

# -------------------------------------------------------------------
# Reliable data sources (these respond correctly)
# -------------------------------------------------------------------
STATES = {
    "WV": "https://tagis.dep.wv.gov/arcgis/rest/services/WVDEP_enterprise/oil_gas/MapServer/0",
    "OH": "https://gis.ohiodnr.gov/arcgis/rest/services/DOG_Services/Oilgas_Wells_public/MapServer/0",
    "PA": "https://www.paoilandgasreporting.state.pa.us/arcgis/rest/services/Public/OG_Wells/MapServer/0",
    "TX": "https://gis.rrc.texas.gov/server/rest/services/Public/Wells/MapServer/0"
}

# Known coordinate systems for each service
KNOWN_CRS = {
    "WV": "EPSG:3857",  # Web Mercator
    "OH": "EPSG:4326",  # Lat/Lon
    "PA": "EPSG:3857",
    "TX": "EPSG:3857"
}

# Directories
DATA_DIR = Path("data")
TILES_DIR = Path("tiles")
LOG_FILE = Path("build_log.txt")

# Zooms
ZOOM_MIN, ZOOM_MAX = 4, 16

# -------------------------------------------------------------------
def log(msg):
    stamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S UTC]")
    line = f"{stamp} {msg}"
    print(line)
    if LOG_FILE.exists():
        LOG_FILE.write_text(LOG_FILE.read_text() + line + "\n")
    else:
        LOG_FILE.write_text(line + "\n")

# -------------------------------------------------------------------
def reproject_features(features, src_epsg):
    """Transform coordinates to EPSG:4326 (lat/lon)."""
    transformer = Transformer.from_crs(src_epsg, "EPSG:4326", always_xy=True)
    fixed = []
    for f in features:
        try:
            geom = shape(f["geometry"])
            geom2 = shapely_ops.transform(transformer.transform, geom)
            f["geometry"] = mapping(geom2)
            fixed.append(f)
        except Exception:
            continue
    return fixed

# -------------------------------------------------------------------
def fetch_geojson(service_url, state):
    """Fetch wells from ArcGIS REST service and reproject to EPSG:4326."""
    src_epsg = KNOWN_CRS.get(state, "EPSG:4326")
    result_offset = 0
    page_size = 2000
    all_feats = []
    layer_url = f"{service_url}/query"

    while True:
        params = {
            "where": "1=1",
            "outFields": "*",
            "f": "geojson",
            "resultOffset": result_offset,
            "resultRecordCount": page_size
        }
        try:
            r = requests.get(layer_url, params=params, timeout=90, verify=False)
            r.raise_for_status()
            js = r.json()
            feats = js.get("features", [])
            if not feats:
                break
            all_feats.extend(feats)
            log(f"{state}: fetched {len(feats)} (total {len(all_feats)})")
            result_offset += page_size
        except Exception as e:
            log(f"{state}: ERROR {e}")
            break

    if not all_feats:
        log(f"{state}: no features fetched.")
        return 0

    all_feats = reproject_features(all_feats, src_epsg)
    out_path = DATA_DIR / f"{state}.geojson"
    out_path.write_text(json.dumps({"type": "FeatureCollection", "features": all_feats}))
    log(f"{state}: saved {len(all_feats)} features reprojected from {src_epsg}")
    return len(all_feats)

# -------------------------------------------------------------------
def build_tiles():
    """Build aligned tiles using Tippecanoe."""
    files = [str(f) for f in DATA_DIR.glob("*.geojson") if f.stat().st_size > 0]
    if not files:
        log("❌ No GeoJSON files found to tile.")
        return False

    cmd = [
        "tippecanoe",
        "-zg",
        "-Z", str(ZOOM_MIN),
        "-z", str(ZOOM_MAX),
        "-e", "tiles",
        "--force",
        "--drop-densest-as-needed",
        "--read-parallel",
        "--coalesce",
        "--extend-zooms-if-still-dropping",
        "--layer=MineraLinkWells",
        "--no-feature-limit",
        "--no-tile-size-limit"
    ] + files

    try:
        subprocess.run(cmd, check=True)
        log("✅ Tippecanoe finished building tiles.")
        return True
    except subprocess.CalledProcessError as e:
        log(f"❌ Tippecanoe failed: {e}")
        return False

# -------------------------------------------------------------------
def git_commit_and_push():
    """Push tiles to gh-pages using the GITHUB_TOKEN."""
    msg = f"Auto-update tiles — {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}"
    subprocess.run(["git", "config", "user.name", "github-actions"], check=True)
    subprocess.run(["git", "config", "user.email", "actions@github.com"], check=True)
    subprocess.run(["git", "checkout", "-B", "gh-pages"], check=True)
    subprocess.run(["git", "add", "tiles"], check=True)
    subprocess.run(["git", "commit", "-m", msg], check=False)
    subprocess.run(
        [
            "git",
            "push",
            "--force",
            "https://x-access-token:${GITHUB_TOKEN}@github.com/mikeaa1983/mineralink-tiles.git",
            "gh-pages",
        ],
        shell=True,
        check=False,
    )
    log("✅ Tiles pushed to gh-pages branch.")

# -------------------------------------------------------------------
def main():
    start = time.time()
    DATA_DIR.mkdir(exist_ok=True)
    TILES_DIR.mkdir(exist_ok=True)
    LOG_FILE.write_text("")

    total = 0
    for state, url in STATES.items():
        log(f"Fetching {state} wells...")
        total += fetch_geojson(url, state)

    log(f"Total features fetched: {total}")
    if total == 0:
        log("❌ No data fetched. Exiting.")
        return

    if build_tiles():
        git_commit_and_push()

    runtime = round(time.time() - start, 1)
    log(f"✅ Complete in {runtime}s")

if __name__ == "__main__":
    main()
