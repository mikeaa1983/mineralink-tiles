#!/usr/bin/env python3
import os, json, requests, subprocess, shutil, time
import geopandas as gpd
from shapely.geometry import Point, Polygon, LineString

OUT_TILES_DIR = "tiles"
TIPPECANOE_CMD = "tippecanoe"
TIPPECANOE_MINZOOM = 4
TIPPECANOE_MAXZOOM = 14

DATASETS = [
    # --- WEST VIRGINIA ---
    {
        "name": "WV_wells",
        "url": "https://tagis.dep.wv.gov/arcgis/rest/services/WVDEP_enterprise/oil_gas/MapServer/0/query",
        "chunk_bbox": [-82.8, 37.0, -77.7, 40.6],
    },
    {
        "name": "WV_parcels",
        "url": "https://services.wvgis.wvu.edu/arcgis/rest/services/Planning_Cadastre/WV_Parcels/MapServer/0/query",
        "chunk_bbox": [-82.8, 37.0, -77.7, 40.6],
    },

    # --- OHIO ---
    {
        "name": "OH_wells",
        "url": "https://gis.ohiodnr.gov/arcgis/rest/services/DOGRM/MapServer/3/query",
        "chunk_bbox": [-84.8, 38.3, -80.5, 42.0],
    },
    {
        "name": "OH_parcels",
        "url": "https://geo.oit.ohio.gov/arcgis/rest/services/Statewide/Parcels/MapServer/0/query",
        "chunk_bbox": [-84.8, 38.3, -80.5, 42.0],
    },

    # --- PENNSYLVANIA ---
    {
        "name": "PA_wells",
        "url": "https://gis.dep.pa.gov/depgisprd/rest/services/OilGas/OG_Well_Locations/MapServer/0/query",
        "chunk_bbox": [-80.6, 39.7, -74.5, 42.5],
    },
    {
        "name": "PA_laterals",
        "url": "https://gis.dep.pa.gov/depgisprd/rest/services/OilGas/OG_Laterals/MapServer/0/query",
        "chunk_bbox": [-80.6, 39.7, -74.5, 42.5],
    },

    # --- TEXAS ---
    {
        "name": "TX_parcels",
        "url": "https://feature.geographic.texas.gov/arcgis/rest/services/Parcels/stratmap25_land_parcels_48/MapServer/0/query",
        "chunk_bbox": [-106.7, 25.7, -93.5, 36.6],
    },
]

def fetch_geojson(dataset):
    """Fetch dataset in 5x5 chunks and convert ESRI geometry → GeoJSON."""
    name, url = dataset["name"], dataset["url"]
    print(f"\n=== Fetching {name} ===")

    bbox = dataset.get("chunk_bbox")
    if not bbox:
        print(f"⚠️ No bbox for {name}, skipping.")
        return None

    xmin, ymin, xmax, ymax = bbox
    x_step = (xmax - xmin) / 5
    y_step = (ymax - ymin) / 5
    all_features = []

    for i in range(5):
        for j in range(5):
            x0, y0 = xmin + i * x_step, ymin + j * y_step
            x1, y1 = x0 + x_step, y0 + y_step
            print(f"  ▸ Chunk {i+1},{j+1} bbox=({x0:.2f},{y0:.2f},{x1:.2f},{y1:.2f})")

            params = {
                "where": "1=1",
                "geometry": f"{x0},{y0},{x1},{y1}",
                "geometryType": "esriGeometryEnvelope",
                "inSR": "4326",
                "spatialRel": "esriSpatialRelIntersects",
                "outFields": "*",
                "returnGeometry": "true",
                "returnExceededLimitFeatures": "true",
                "f": "json",
                "outSR": "4326",
            }

            for attempt in range(2):
                try:
                    resp = requests.get(url, params=params, timeout=120)
                    if resp.status_code == 404:
                        raise Exception("404 Not Found")
                    if resp.status_code >= 500:
                        raise Exception("Server error")
                    data = resp.json()
                    feats = data.get("features", [])
                    if feats:
                        all_features.extend(feats)
                        print(f"    + {len(feats)} features")
                    break
                except Exception as e:
                    print(f"⚠️ Chunk {i+1},{j+1} attempt {attempt+1} failed: {e}")
                    time.sleep(3)
                    continue

    if not all_features:
        print(f"⚠️ No geometries found in {name}")
        return None

    features = []
    for feat in all_features:
        geom = feat.get("geometry")
        props = feat.get("attributes", {})
        if not geom:
            continue
        try:
            if "x" in geom and "y" in geom:
                geometry = Point(geom["x"], geom["y"])
            elif "points" in geom:
                geometry = LineString(geom["points"])
            elif "paths" in geom:
                geometry = LineString(geom["paths"][0])
            elif "rings" in geom:
                geometry = Polygon(geom["rings"][0])
            else:
                continue

            gdf = gpd.GeoDataFrame([props], geometry=[geometry], crs="EPSG:4326")
            geojson = json.loads(gdf.to_json())
            features.append(geojson["features"][0])
        except Exception as e:
            print(f"⚠️ Geometry parse error: {e}")

    if not features:
        print(f"⚠️ {name} contained no valid geometries.")
        return None

    geo = {"type": "FeatureCollection", "features": features}
    file_name = f"{name}.geojson"
    with open(file_name, "w", encoding="utf-8") as f:
        json.dump(geo, f)
    print(f"✅ Saved {file_name} ({len(features)} features)")
    return file_name

def build_tiles(name, geojson_file):
    """Run Tippecanoe to create vector tiles."""
    if not geojson_file or not os.path.exists(geojson_file):
        print(f"⚠️ Missing GeoJSON for {name}")
        return False

    with open(geojson_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not data.get("features"):
        print(f"⚠️ {name} empty, skipping Tippecanoe.")
        return False

    out_dir = os.path.join(OUT_TILES_DIR, name)
    os.makedirs(out_dir, exist_ok=True)

    cmd = [
        TIPPECANOE_CMD,
        "--output-to-directory", out_dir,
        "--layer", name,
        "--minimum-zoom", str(TIPPECANOE_MINZOOM),
        "--maximum-zoom", str(TIPPECANOE_MAXZOOM),
        "--force",
        geojson_file,
    ]

    try:
        subprocess.run(cmd, check=True)
        print(f"✅ Tiles built: {out_dir}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Tippecanoe failed for {name}: {e}")
        return False

def main():
    print("=== Starting Mineralink Tile Builder ===")
    os.makedirs(OUT_TILES_DIR, exist_ok=True)
    built_layers = []

    for ds in DATASETS:
        f = fetch_geojson(ds)
        if not f:
            continue
        success = build_tiles(ds["name"], f)
        if success:
            built_layers.append(ds["name"])

    if built_layers:
        print(f"\n✅ Successfully built {len(built_layers)} layers:")
        for layer in built_layers:
            print(f"   • {layer}")
    else:
        print("\n⚠️ No tiles were built — all endpoints failed or returned empty data.")

    print(f"\nTiles directory: {os.path.abspath(OUT_TILES_DIR)}")

if __name__ == "__main__":
    main()
