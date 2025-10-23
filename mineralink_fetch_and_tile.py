#!/usr/bin/env python3
"""
MineraLink – Automated Well & Parcel Data Fetcher + Tile Builder
Auto-reprojects each state’s coordinate system to WGS84 (EPSG:4326)
"""

import os, json, time, subprocess, requests
from datetime import datetime
from pathlib import Path
from shapely.geometry import shape, mapping
from shapely import ops as shapely_ops
from pyproj import Transformer

# -------------------------------------------------------------------
# ArcGIS Services & known CRS codes
# -------------------------------------------------------------------
STATES = {
    "WV": ("https://tagis.dep.wv.gov/arcgis/rest/services/WVDEP_enterprise/oil_gas/MapServer/0", "EPSG:4326"),
    "OH": ("https://gis.ohiodnr.gov/arcgis/rest/services/DOG_Services/Oilgas_Wells_public/MapServer/0", "EPSG:4326"),
    "PA": ("https://www.paoilandgasreporting.state.pa.us/arcgis/rest/services/Public/OG_Wells/MapServer/0", "EPSG:2272"),  # PA South feet
    "TX": ("https://gis.rrc.texas.gov/server/rest/services/Public/Wells/MapServer/0", "EPSG:3857"),
    "CO": ("https://cogccmap.state.co.us/arcgis/rest/services/ogcc_wells/MapServer/0", "EPSG:3857"),
    "OK": ("https://services.arcgis.com/ZOQj2K1Q2qRZmbVZ/arcgis/rest/services/OCC_Wells/MapServer/0", "EPSG:4326"),
    "LA": ("https://gis.sonris.com/arcgis/rest/services/WellData/MapServer/0", "EPSG:3857"),
    "NM": ("https://ocdimage.emnrd.state.nm.us/arcgis/rest/services/public/OCD_Wells/MapServer/0", "EPSG:3857"),
    "AR": ("https://gis.arkansas.gov/arcgis/rest/services/OilGas/MapServer/0", "EPSG:3857"),
    "WY": ("https://wsgs.maps.arcgis.com/arcgis/rest/services/Wyoming_OilGasWells/MapServer/0", "EPSG:3857"),
    "UT": ("https://mapserv.utah.gov/arcgis/rest/services/OGM/Wells/MapServer/0", "EPSG:3857")
}

DATA_DIR = Path("data")
TILES_DIR = Path("tiles")
LOG_FILE = Path("build_log.txt")
ZOOM_MIN, ZOOM_MAX = 4, 16

# -------------------------------------------------------------------
def log(msg):
    stamp = datetime.utcnow().strftime("[%Y-%m-%d %H:%M:%S UTC]")
    print(f"{stamp} {msg}")
    LOG_FILE.write_text(LOG_FILE.read_text() + f"{stamp} {msg}\n" if LOG_FILE.exists() else f"{stamp} {msg}\n")

def reproject_geojson(features, src_epsg, dst_epsg="EPSG:4326"):
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

def fetch_features(url, epsg, state):
    all_features = []
    offset, page_size = 0, 2000
    qurl = f"{url}/query"
    while True:
        p = {"where": "1=1", "outFields": "*", "f": "geojson",
             "resultOffset": offset, "resultRecordCount": page_size}
        try:
            r = requests.get(qurl, params=p, timeout=60, verify=False)
            r.raise_for_status()
            js = r.json()
            if "features" not in js or not js["features"]:
                break
            all_features.extend(js["features"])
            log(f"{state}: +{len(js['features'])} (total {len(all_features)})")
            offset += page_size
        except Exception as e:
            log(f"{state}: ERROR {e}")
            break

    if not all_features:
        log(f"{state}: No data")
        return 0

    feats = reproject_geojson(all_features, epsg)
    out = {"type": "FeatureCollection", "features": feats}
    fpath = DATA_DIR / f"{state}.geojson"
    fpath.write_text(json.dumps(out))
    log(f"{state}: saved {len(feats)} features to {fpath}")
    return len(feats)

def build_tiles():
    files = [str(f) for f in DATA_DIR.glob("*.geojson") if f.stat().st_size > 500]
    if not files:
        log("❌ No valid data files.")
        return False
    cmd = [
        "tippecanoe", "-zg", "-Z", str(ZOOM_MIN), "-z", str(ZOOM_MAX),
        "-e", "tiles", "--force", "--drop-densest-as-needed", "--read-parallel",
        "--coalesce", "--extend-zooms-if-still-dropping",
        "--layer=MineraLinkWells", "--no-feature-limit", "--no-tile-size-limit"
    ] + files
    try:
        subprocess.run(cmd, check=True)
        log("✅ Tippecanoe success.")
        return True
    except subprocess.CalledProcessError as e:
        log(f"❌ Tippecanoe failed: {e}")
        return False

def git_push():
    msg = f"Auto-update tiles {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
    subprocess.run(["git", "config", "user.name", "github-actions"], check=True)
    subprocess.run(["git", "config", "user.email", "actions@github.com"], check=True)
    subprocess.run(["git", "checkout", "-B", "gh-pages"], check=True)
    subprocess.run(["git", "add", "tiles"], check=True)
    subprocess.run(["git", "commit", "-m", msg], check=True)
    subprocess.run(["git", "push", "-f", "origin", "gh-pages"], check=True)
    log("✅ Tiles pushed to gh-pages.")

def main():
    start = time.time()
    DATA_DIR.mkdir(exist_ok=True)
    TILES_DIR.mkdir(exist_ok=True)
    LOG_FILE.write_text("")

    total = 0
    for state, (url, epsg) in STATES.items():
        log(f"Fetching {state} wells...")
        total += fetch_features(url, epsg, state)

    log(f"Total fetched: {total}")
    if total == 0:
        log("❌ Nothing fetched. Exiting.")
        return

    if build_tiles():
        try:
            git_push()
        except Exception as e:
            log(f"⚠️ Git push failed: {e}")
    else:
        log("⚠️ Build failed.")

    log(f"✅ Done in {round(time.time() - start,1)}s")

if __name__ == "__main__":
    main()
