#!/usr/bin/env python3
"""
MineraLink – Stable Well & Parcel Tile Builder (Fixed Projection)
Author: mikeaa1983
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
# Reliable sources only
# -------------------------------------------------------------------
STATES = {
    "WV": "https://tagis.dep.wv.gov/arcgis/rest/services/WVDEP_enterprise/oil_gas/MapServer/0",
    "OH": "https://gis.ohiodnr.gov/arcgis/rest/services/DOG_Services/Oilgas_Wells_public/MapServer/0",
    "PA": "https://www.paoilandgasreporting.state.pa.us/arcgis/rest/services/Public/OG_Wells/MapServer/0",
    "TX": "https://gis.rrc.texas.gov/server/rest/services/Public/Wells/MapServer/0"
}

KNOWN_CRS = {
    "WV": "EPSG:3857",
    "OH": "EPSG:4326",
    "PA": "EPSG:3857",
    "TX": "EPSG:3857"
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
def reproject_and_fix_coords(features, src_epsg):
    """Convert coordinates to EPSG:4326 and ensure correct lon/lat order."""
    transformer = Transformer.from_crs(src_epsg, "EPSG:4326", always_xy=True)
    fixed = []
    for f in features:
        try:
            geom = shape(f["geometry"])
            geom2 = shapely_ops.transform(transformer.transform, geom)
            # Flip if lat/lon inverted
            coords = mapping(geom2)
            if coords["type"] == "Point":
                x, y = coords["coordinates"]
                if abs(y) > 90 or abs(x) > 180:  # invalid range check
                    x, y = y, x
                coords["coordinates"] = [x, y]
            f["geometry"] = coords
            fixed.append(f)
        except Exception as e:
            continue
    return fixed

# -------------------------------------------------------------------
def fetch_geojson(service_url, state):
    """Fetch and normalize well data."""
    src_epsg = KNOWN_CRS.get(state, "EPSG:4326")
    page, size, allf = 0, 2000, []
    url = f"{service_url}/query"

    while True:
        params = {
            "where": "1=1",
            "outFields": "*",
            "f": "geojson",
            "resultOffset": page * size,
            "resultRecordCount": size
        }
        try:
            r = requests.get(url, params=params, timeout=90, verify=False)
            r.raise_for_status()
            js = r.json()
            feats = js.get("features", [])
            if not feats:
                break
            allf.extend(feats)
            log(f"{state}: +{len(feats)} features ({len(allf)} total)")
            page += 1
        except Exception as e:
            log(f"{state}: ERROR {e}")
            break

    if not allf:
        log(f"{state}: no features fetched")
        return 0

    fixed = reproject_and_fix_coords(allf, src_epsg)
    out = DATA_DIR / f"{state}.geojson"
    out.write_text(json.dumps({"type": "FeatureCollection", "features": fixed}))
    log(f"{state}: saved {len(fixed)} corrected features")
    return len(fixed)

# -------------------------------------------------------------------
def build_tiles():
    files = [str(f) for f in DATA_DIR.glob("*.geojson") if f.stat().st_size > 0]
    if not files:
        log("❌ No valid GeoJSON files.")
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
        log("✅ Tippecanoe built clean tiles.")
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
    log("✅ Tiles pushed to gh-pages.")

# -------------------------------------------------------------------
def main():
    start = time.time()
    DATA_DIR.mkdir(exist_ok=True)
    TILES_DIR.mkdir(exist_ok=True)
    if LOG_FILE.exists(): LOG_FILE.unlink()

    total = 0
    for s, u in STATES.items():
        log(f"Fetching {s} wells...")
        total += fetch_geojson(u, s)

    if total == 0:
        log("❌ No data fetched.")
        return

    if build_tiles():
        git_push()

    log(f"✅ Done in {round(time.time()-start,1)}s")

if __name__ == "__main__":
    main()
