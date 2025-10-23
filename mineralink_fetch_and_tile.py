#!/usr/bin/env python3
"""
MineraLink – Automated Well & Parcel Data Fetcher + Tile Builder
Author: mikeaa1983
Runs in GitHub Actions nightly to update vector tiles for GitHub Pages.
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
# Configuration
# -------------------------------------------------------------------
STATES = {
    "WV": "https://tagis.dep.wv.gov/arcgis/rest/services/WVDEP_enterprise/oil_gas/MapServer/0",
    "OH": "https://gis.ohiodnr.gov/arcgis/rest/services/DOG_Services/Oilgas_Wells_public/MapServer/0",
    "PA": "https://www.paoilandgasreporting.state.pa.us/arcgis/rest/services/Public/OG_Wells/MapServer/0",
    "TX": "https://gis.rrc.texas.gov/server/rest/services/Public/Wells/MapServer/0",
    "CO": "https://cogccmap.state.co.us/arcgis/rest/services/ogcc_wells/MapServer/0",
    "OK": "https://services.arcgis.com/ZOQj2K1Q2qRZmbVZ/arcgis/rest/services/OCC_Wells/MapServer/0",
    "LA": "https://gis.sonris.com/arcgis/rest/services/WellData/MapServer/0",
    "NM": "https://ocdimage.emnrd.state.nm.us/arcgis/rest/services/public/OCD_Wells/MapServer/0",
    "AR": "https://gis.arkansas.gov/arcgis/rest/services/OilGas/MapServer/0",
    "WY": "https://wsgs.maps.arcgis.com/arcgis/rest/services/Wyoming_OilGasWells/MapServer/0",
    "UT": "https://mapserv.utah.gov/arcgis/rest/services/OGM/Wells/MapServer/0"
}

ZOOM_MIN, ZOOM_MAX = 4, 16
DATA_DIR = Path("data")
TILES_DIR = Path("tiles")
LOG_FILE = Path("build_log.txt")

# -------------------------------------------------------------------
# Utility
# -------------------------------------------------------------------
def log(msg: str):
    """Print and write a timestamped log message."""
    stamp = datetime.utcnow().strftime("[%Y-%m-%d %H:%M:%S UTC]")
    line = f"{stamp} {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# -------------------------------------------------------------------
# Reproject helper
# -------------------------------------------------------------------
def reproject_geojson(features, src_epsg="EPSG:3857", dst_epsg="EPSG:4326"):
    """Convert feature coordinates to WGS84 lat/lon."""
    transformer = Transformer.from_crs(src_epsg, dst_epsg, always_xy=True)
    new_features = []
    for f in features:
        try:
            geom = shape(f["geometry"])
            xformed = shapely_ops.transform(transformer.transform, geom)
            f["geometry"] = mapping(xformed)
            new_features.append(f)
        except Exception:
            continue
    return new_features

# -------------------------------------------------------------------
# Fetch ArcGIS data
# -------------------------------------------------------------------
def fetch_features(service_url: str, state: str) -> int:
    """Fetch all features from a public ArcGIS REST service using pagination."""
    all_features = []
    result_offset = 0
    page_size = 2000
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
            r = requests.get(layer_url, params=params, timeout=60, verify=False)
            r.raise_for_status()
            data = r.json()
            if "features" not in data or not data["features"]:
                break
            all_features.extend(data["features"])
            log(f"{state}: fetched {len(data['features'])} (total {len(all_features)})")
            result_offset += page_size
        except Exception as e:
            log(f"{state}: ERROR {e}")
            break

    if not all_features:
        log(f"{state}: ❌ No features returned, skipping.")
        return 0

    # Reproject to WGS84
    all_features = reproject_geojson(all_features)

    geojson = {"type": "FeatureCollection", "features": all_features}
    out_file = DATA_DIR / f"{state}.geojson"
    try:
        out_file.write_text(json.dumps(geojson))
        log(f"{state}: ✅ Saved {len(all_features)} features to {out_file}")
    except Exception as e:
        log(f"{state}: ❌ Failed to write file: {e}")
        return 0
    return len(all_features)

# -------------------------------------------------------------------
# Build Tippecanoe tiles
# -------------------------------------------------------------------
def build_tiles():
    """Run Tippecanoe to build vector tiles."""
    files = [f for f in DATA_DIR.glob("*.geojson") if f.stat().st_size > 500]
    if not files:
        log("❌ No valid GeoJSON files found — skipping tile build.")
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
        "--no-feature-limit",
        "--no-tile-size-limit",
        "--layer=MineraLinkWells",
    ] + [str(f) for f in files]

    try:
        subprocess.run(cmd, check=True)
        log("✅ Tippecanoe completed successfully.")
        return True
    except subprocess.CalledProcessError as e:
        log(f"❌ Tippecanoe failed: {e}")
        return False

# -------------------------------------------------------------------
# Commit and push tiles
# -------------------------------------------------------------------
def git_commit_and_push():
    """Commit and push tiles to gh-pages."""
    msg = f"Auto-update tiles — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
    subprocess.run(["git", "config", "user.name", "github-actions"], check=True)
    subprocess.run(["git", "config", "user.email", "actions@github.com"], check=True)
    subprocess.run(["git", "checkout", "-B", "gh-pages"], check=True)
    subprocess.run(["git", "add", "tiles"], check=True)
    subprocess.run(["git", "commit", "-m", msg], check=True)
    subprocess.run(["git", "push", "-f", "origin", "gh-pages"], check=True)
    log("Tiles committed and pushed to gh-pages.")

# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------
def main():
    start = time.time()
    DATA_DIR.mkdir(exist_ok=True)
    TILES_DIR.mkdir(exist_ok=True)
    LOG_FILE.write_text("")

    total = 0
    for state, url in STATES.items():
        log(f"Fetching {state} wells...")
        count = fetch_features(url, state)
        total += count
        log(f"{state}: total {count}")

    log(f"Total features fetched: {total}")
    if total == 0:
        log("❌ No data fetched. Skipping build and push.")
        return

    if build_tiles():
        try:
            git_commit_and_push()
        except Exception as e:
            log(f"⚠️ Git push failed: {e}")
    else:
        log("⚠️ Tile build failed; skipping push.")

    runtime = round(time.time() - start, 1)
    log(f"✅ Complete in {runtime}s")

if __name__ == "__main__":
    main()
