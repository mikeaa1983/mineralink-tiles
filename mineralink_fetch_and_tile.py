#!/usr/bin/env python3
import os, json, requests, subprocess, time
from shapely.geometry import Point, Polygon, LineString
import geopandas as gpd

OUT_TILES_DIR = "tiles"
FALLBACK_DIR = "fallback_data"
TIPPECANOE = "tippecanoe"

# Each dataset will try for 5 minutes max
MAX_DATASET_SECONDS = 300
REQUEST_TIMEOUT = 30

DATASETS = [
    {"name": "WV_wells", "url": "https://tagis.dep.wv.gov/arcgis/rest/services/WVDEP_enterprise/oil_gas/MapServer/0/query", "bbox": [-82.8, 37.0, -77.7, 40.6]},
    {"name": "OH_parcels", "url": "https://geo.oit.ohio.gov/arcgis/rest/services/Statewide/Parcels/MapServer/0/query", "bbox": [-84.8, 38.3, -80.5, 42.0]},
    {"name": "TX_parcels", "url": "https://feature.geographic.texas.gov/arcgis/rest/services/Parcels/stratmap25_land_parcels_48/MapServer/0/query", "bbox": [-106.7, 25.7, -93.5, 36.6]},
]

def fetch_geojson(ds):
    name, url = ds["name"], ds["url"]
    print(f"\n=== Fetching {name} ===")
    start = time.time()
    features = []
    xmin, ymin, xmax, ymax = ds["bbox"]
    for i in range(5):
        for j in range(5):
            if time.time() - start > MAX_DATASET_SECONDS:
                print(f"⏱️ Timeout for {name}")
                return None
            x0, y0 = xmin + (xmax - xmin) / 5 * i, ymin + (ymax - ymin) / 5 * j
            x1, y1 = x0 + (xmax - xmin) / 5, y0 + (ymax - ymin) / 5
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
                    print(f"  +{len(feats)} features")
            except Exception as e:
                print(f"⚠️ Chunk {i+1},{j+1} failed: {e}")

    if not features:
        print(f"⚠️ No geometries for {name}")
        return None

    out_path = f"{name}.geojson"
    gdf = []
    for f in features:
        geom = f.get("geometry")
        props = f.get("attributes", {})
        if geom:
            try:
                if "x" in geom and "y" in geom:
                    geo = Point(geom["x"], geom["y"])
                elif "rings" in geom:
                    geo = Polygon(geom["rings"][0])
                elif "paths" in geom:
                    geo = LineString(geom["paths"][0])
                else:
                    continue
                gdf.append({"geometry": geo, **props})
            except:
                continue
    if not gdf:
        print(f"⚠️ {name} parse failed")
        return None

    gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs="EPSG:4326")
    gdf.to_file(out_path, driver="GeoJSON")
    return out_path


def build_tiles(name, geojson):
    if not geojson or not os.path.exists(geojson):
        print(f"⚠️ No GeoJSON for {name}, skipping")
        return False
    os.makedirs(OUT_TILES_DIR, exist_ok=True)
    cmd = [
        TIPPECANOE, "--output-to-directory", f"{OUT_TILES_DIR}/{name}",
        "--layer", name, "--force", geojson,
        "--minimum-zoom=4", "--maximum-zoom=14"
    ]
    try:
        subprocess.run(cmd, check=True)
        print(f"✅ Built tiles for {name}")
        return True
    except Exception as e:
        print(f"⚠️ Tippecanoe failed for {name}: {e}")
        return False


def main():
    print("=== Starting build ===")
    os.makedirs(OUT_TILES_DIR, exist_ok=True)
    built = []

    for ds in DATASETS:
        geojson = fetch_geojson(ds)
        if not geojson:
            fallback = os.path.join(FALLBACK_DIR, f"{ds['name']}.geojson")
            if os.path.exists(fallback):
                print(f"🧩 Using fallback for {ds['name']}")
                geojson = fallback
            else:
                print(f"⚠️ No fallback for {ds['name']}, skipping.")
                continue
        if build_tiles(ds["name"], geojson):
            built.append(ds["name"])

    if built:
        print(f"✅ Done: {built}")
    else:
        print("⚠️ No tiles built at all!")

    print(f"Tiles dir: {OUT_TILES_DIR}")

if __name__ == "__main__":
    main()
