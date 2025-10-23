#!/usr/bin/env python3
"""
MineraLink – Final Fixed Tile Builder
Correctly reprojects all state well datasets to EPSG:4326 (lat/lon).
"""

import os
import json
import time
import subprocess
import requests
from datetime import datetime
from pathlib import Path
from shapely.geometry import shape, mapping
from shapely.ops import transform as shp_transform
from pyproj import Transformer

# -------------------------------------------------------------------
# Known working ArcGIS endpoints
# -------------------------------------------------------------------
STATES = {
    "WV": ("https://tagis.dep.wv.gov/arcgis/rest/services/WVDEP_enterprise/oil_gas/MapServer/0", "EPSG:3857"),
    "OH": ("https://gis.ohiodnr.gov/arcgis/rest/services/DOG_Services/Oilgas_Wells_public/MapServer/0", "EPSG:4326"),
    "PA": ("https://www.paoilandgasreporting.state.pa.us/arcgis/rest/services/Public/OG_Wells/MapServer/0", "EPSG:3857"),
    "TX": ("https://gis.rrc.texas.gov/server/rest/services/Public/Wells/MapServer/0", "EPSG:3857")
}

DATA_DIR = Path("data")
TILES_DIR = Path("tiles")
LOG_FILE = Path("build_log.txt")
ZOOM_MIN, ZOOM_MAX = 4, 16

# -------------------------------------------------------------------
def log(msg):
    stamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S UTC]")
    print(f"{stamp} {msg}")
    with open(LOG_FILE, "a") as f:
        f.write(f"{stamp} {msg}\n")

# -------------------------------------------------------------------
def fetch_geojson(service_url, src_epsg, state):
    """Fetch wells from ArcGIS and reproject all to EPSG:4326."""
    layer_url = f"{service_url}/query"
    features = []
    result_offset = 0
    page_size = 2000

    transformer = Transformer.from_crs(src_epsg, "EPSG:4326", always_xy=True)

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
            for f in feats:
                try:
                    geom = shape(f["geometry"])
                    geom4326 = shp_transform(transformer.transform, geom)
                    f["geometry"] = mapping(geom4326)
                    features.append(f)
                except Exception:
                    continue
            result_offset += page_size
            log(f"{state}: fetched {len(feats)} (total {len(features)})")
        except Exception as e:
            log(f"{state}: ERROR {e}")
            break

    if not features:
        log(f"{state}: no valid features")
        return 0

    out_path = DATA_DIR / f"{state}.geojson"
    out_path.write_text(json.dumps({"type": "FeatureCollection", "features": features}))
    log(f"{state}: saved {len(features)} reprojected features from {src_epsg}")
    return len(features)

# -------------------------------------------------------------------
def build_tiles():
    files = [str(f) for f in DATA_DIR.glob("*.geojson") if f.stat().st_size > 0]
    if not files:
        log("❌ No valid GeoJSON files to build.")
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
        log("✅ Tippecanoe built proper WGS84 tiles.")
        return True
    except subprocess.CalledProcessError as e:
        log(f"❌ Tippecanoe failed: {e}")
        return False

# -------------------------------------------------------------------
def git_push():
    msg = f"Auto-update tiles — {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}"
    subprocess.run(["git", "config", "user.name", "github-actions"], check=True)
    subprocess.run(["git", "config", "user.email", "actions@github.com"], check=True)
    subprocess.run(["git", "checkout", "-B", "gh-pages"], check=True)
    subprocess.run(["git", "add", "tiles"], check=True)
    subprocess.run(["git", "commit", "-m", msg], check=False)
    subprocess.run([
        "git", "push", "--force",
        "https://x-access-token:${GITHUB_TOKEN}@github.com/mikeaa1983/mineralink-tiles.git",
        "gh-pages"
    ], shell=True, check=False)
    log("✅ Tiles pushed to gh-pages branch.")

# -------------------------------------------------------------------
def main():
    start = time.time()
    DATA_DIR.mkdir(exist_ok=True)
    TILES_DIR.mkdir(exist_ok=True)
    if LOG_FILE.exists(): LOG_FILE.unlink()

    total = 0
    for state, (url, epsg) in STATES.items():
        log(f"Fetching {state} wells...")
        total += fetch_geojson(url, epsg, state)

    if total == 0:
        log("❌ No data fetched; exiting.")
        return

    if build_tiles():
        git_push()

    log(f"✅ Complete in {round(time.time()-start,1)}s")

if __name__ == "__main__":
    main()
