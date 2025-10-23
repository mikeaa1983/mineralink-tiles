#!/usr/bin/env python3
"""
MineraLink – Automated Well & Parcel Data Fetcher + Tile Builder
Author: mikeaa1983 (GitHub)
Runs in GitHub Actions nightly to update vector tiles for GitHub Pages.
"""

import os
import json
import time
import subprocess
import requests
from datetime import datetime
from pathlib import Path

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
# Utility functions
# -------------------------------------------------------------------
def log(msg):
    """Print and write a timestamped log message."""
    stamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S UTC]")
    line = f"{stamp} {msg}"
    print(line)
    LOG_FILE.write_text(LOG_FILE.read_text() + line + "\n" if LOG_FILE.exists() else line + "\n")

# -------------------------------------------------------------------
# Fetch ArcGIS data
# -------------------------------------------------------------------
def fetch_features(service_url, state):
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
            # Disable SSL verification for servers with bad certs (e.g., PA)
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

    geojson = {"type": "FeatureCollection", "features": all_features}
    out_file = DATA_DIR / f"{state}.geojson"
    out_file.write_text(json.dumps(geojson))
    return len(all_features)

# -------------------------------------------------------------------
# Build Tippecanoe tiles
# -------------------------------------------------------------------
def build_tiles():
    """Run tippecanoe to build vector tiles."""
    cmd = [
    "tippecanoe",
    "-o", "minerals.mbtiles",
    "-zg",
    "-Z", "4",
    "-z", "16",
    "-e", "tiles",
    "--force",
    "--drop-densest-as-needed",
    "--read-parallel",
    "--coalesce",
    "--extend-zooms-if-still-dropping",
    "--layer=MineraLinkWells",  # <--- Important fixed layer name
    "--no-feature-limit",
    "--no-tile-size-limit",
    "data/WV.geojson", "data/OH.geojson"
]
subprocess.run(cmd, check=True)


# -------------------------------------------------------------------
# Commit and push tiles to gh-pages
# -------------------------------------------------------------------
def git_commit_and_push():
    """Commit and push tiles to gh-pages branch."""
    msg = f"Auto-update tiles — {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}"
    subprocess.run(["git", "config", "user.name", "github-actions"], check=True)
    subprocess.run(["git", "config", "user.email", "actions@github.com"], check=True)
    subprocess.run(["git", "checkout", "-B", "gh-pages"], check=True)
    subprocess.run(["git", "add", "tiles"], check=True)
    subprocess.run(["git", "commit", "-m", msg], check=True)
    subprocess.run(["git", "push", "-f", "origin", "gh-pages"], check=True)
    log("Tiles committed and pushed to gh-pages branch.")

# -------------------------------------------------------------------
# Main execution
# -------------------------------------------------------------------
def main():
    start = time.time()
    DATA_DIR.mkdir(exist_ok=True)
    TILES_DIR.mkdir(exist_ok=True)
    LOG_FILE.write_text("")

    total_features = 0
    for state, url in STATES.items():
        log(f"Fetching {state} wells...")
        count = fetch_features(url, state)
        total_features += count
        log(f"{state}: total features {count}")

    log(f"Total features fetched: {total_features}")
    if total_features == 0:
        log("No data fetched; skipping tile build.")
        return

    build_tiles()
    git_commit_and_push()

    runtime = round(time.time() - start, 1)
    log(f"✅ Complete in {runtime}s")

if __name__ == "__main__":
    main()
