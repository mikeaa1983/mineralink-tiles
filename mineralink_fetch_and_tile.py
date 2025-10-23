#!/usr/bin/env python3
"""
MineraLink – Auto-fetch, auto-detect CRS, and tile U.S. well data.
Fully self-healing: detects each ArcGIS layer’s CRS automatically.
"""

import os, json, time, subprocess, requests
from datetime import datetime
from pathlib import Path
from shapely.geometry import shape, mapping
from shapely import ops as shapely_ops
from pyproj import Transformer

# -------------------------------------------------------------------
# ArcGIS layers to include
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

DATA_DIR = Path("data")
TILES_DIR = Path("tiles")
LOG_FILE = Path("build_log.txt")
ZOOM_MIN, ZOOM_MAX = 4, 16

# -------------------------------------------------------------------
def log(msg: str):
    stamp = datetime.utcnow().strftime("[%Y-%m-%d %H:%M:%S UTC]")
    print(f"{stamp} {msg}")
    text = f"{stamp} {msg}\n"
    LOG_FILE.write_text(LOG_FILE.read_text() + text if LOG_FILE.exists() else text)

# -------------------------------------------------------------------
def detect_wkid(service_url: str) -> str:
    """Query ArcGIS layer metadata and return its WKID as an EPSG code string."""
    try:
        r = requests.get(f"{service_url}?f=pjson", timeout=30, verify=False)
        r.raise_for_status()
        js = r.json()
        wkid = None
        if "extent" in js and "spatialReference" in js["extent"]:
            sr = js["extent"]["spatialReference"]
            wkid = sr.get("latestWkid") or sr.get("wkid")
        if not wkid and "spatialReference" in js:
            wkid = js["spatialReference"].get("latestWkid") or js["spatialReference"].get("wkid")
        if not wkid:
            return "EPSG:4326"
        epsg = f"EPSG:{wkid}"
        log(f"Detected CRS {epsg} for {service_url}")
        return epsg
    except Exception as e:
        log(f"⚠️ Could not detect CRS for {service_url}: {e}")
        return "EPSG:4326"

# -------------------------------------------------------------------
def reproject_features(features, src_epsg):
    """Transform coordinates to EPSG:4326."""
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
    """Fetch all features from an ArcGIS REST layer with pagination and reprojection."""
    src_epsg = detect_wkid(service_url)
    result_offset, page, all_feats = 0, 2000, []
    qurl = f"{service_url}/query"

    while True:
        params = {"where": "1=1", "outFields": "*", "f": "geojson",
                  "resultOffset": result_offset, "resultRecordCount": page}
        try:
            r = requests.get(qurl, params=params, timeout=90, verify=False)
            r.raise_for_status()
            js = r.json()
            feats = js.get("features", [])
            if not feats:
                break
            all_feats.extend(feats)
            log(f"{state}: +{len(feats)} (total {len(all_feats)})")
            result_offset += page
        except Exception as e:
            log(f"{state}: ERROR {e}")
            break

    if not all_feats:
        log(f"{state}: No features fetched")
        return 0

    all_feats = reproject_features(all_feats, src_epsg)
    out = {"type": "FeatureCollection", "features": all_feats}
    out_path = DATA_DIR / f"{state}.geojson"
    out_path.write_text(json.dumps(out))
    log(f"{state}: saved {len(all_feats)} reprojected features to {out_path}")
    return len(all_feats)

# -------------------------------------------------------------------
def build_tiles():
    """Run Tippecanoe and build aligned vector tiles."""
    files = [str(f) for f in DATA_DIR.glob("*.geojson") if f.stat().st_size > 1000]
    if not files:
        log("❌ No valid GeoJSON files.")
        return False
    cmd = [
        "tippecanoe", "-zg", "-Z", str(ZOOM_MIN), "-z", str(ZOOM_MAX),
        "-e", "tiles", "--force", "--drop-densest-as-needed",
        "--read-parallel", "--coalesce", "--extend-zooms-if-still-dropping",
        "--layer=MineraLinkWells", "--no-feature-limit", "--no-tile-size-limit"
    ] + files
    try:
        subprocess.run(cmd, check=True)
        log("✅ Tippecanoe finished building tiles.")
        return True
    except subprocess.CalledProcessError as e:
        log(f"❌ Tippecanoe failed: {e}")
        return False

# -------------------------------------------------------------------
def git_push():
    msg = f"Auto-update tiles {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
    subprocess.run(["git", "config", "user.name", "github-actions"], check=True)
    subprocess.run(["git", "config", "user.email", "actions@github.com"], check=True)
    subprocess.run(["git", "checkout", "-B", "gh-pages"], check=True)
    subprocess.run(["git", "add", "tiles"], check=True)
    subprocess.run(["git", "commit", "-m", msg], check=True)
    subprocess.run(["git", "push", "-f", "origin", "gh-pages"], check=True)
    log("✅ Tiles pushed to gh-pages.")

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
        try:
            git_push()
        except Exception as e:
            log(f"⚠️ Git push failed: {e}")

    runtime = round(time.time() - start, 1)
    log(f"✅ Complete in {runtime}s")

if __name__ == "__main__":
    main()
