#!/usr/bin/env python3
import os, json, requests, subprocess, shutil
import geopandas as gpd

# ============================================================
# CONFIGURATION
# ============================================================
OUT_TILES_DIR = "tiles"
TIPPECANOE_CMD = "tippecanoe"
TIPPECANOE_MINZOOM = 4
TIPPECANOE_MAXZOOM = 14

# ============================================================
# DATASETS (Wells + Parcels by State)
# ============================================================
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
        "url": "https://gis.ohiodnr.gov/arcgis/rest/services/DOG_Services/MapServer/0/query",
        "chunk_bbox": [-84.8, 38.3, -80.5, 42.0],
    },
    {
        "name": "OH_parcels",
        "url": "https://gis1.oit.ohio.gov/arcgis/rest/services/Statewide/Parcels/MapServer/0/query",
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
    {
        "name": "PA_parcels",
        "url": "https://gis.dep.pa.gov/depgisprd/rest/services/Boundaries/County_Boundaries/MapServer/0/query",
        "chunk_bbox": [-80.6, 39.7, -74.5, 42.5],
    },

    # --- TEXAS ---
    {
        "name": "TX_wells",
        "url": "https://rrc-txdigital.maps.arcgis.com/sharing/rest/content/items/5a28b3085edb47bfa8f35e6d8a3124b8/data",
        "chunk_bbox": [-106.7, 25.7, -93.5, 36.6],
    },
    {
        "name": "TX_parcels",
        "url": "https://feature.geographic.texas.gov/arcgis/rest/services/Parcels/stratmap25_land_parcels_48/MapServer/0/query",
        "chunk_bbox": [-106.7, 25.7, -93.5, 36.6],
    },
]

# ============================================================
# FETCH + CONVERT FUNCTIONS
# ============================================================
def fetch_geojson(dataset):
    """Fetch dataset in 5x5 chunks to avoid ArcGIS timeouts."""
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
            print(f"  ▸ Chunk {i+1}, {j+1} bbox=({x0:.2f},{y0:.2f},{x1:.2f},{y1:.2f})")

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
                resp = requests.get(url, params=params, timeout=60)
                resp.raise_for_status()
                data = resp.json()
                feats = data.get("features", [])
                if feats:
                    all_features.extend(feats)
                    print(f"    + {len(feats)} features")
            except Exception as e:
                print(f"⚠️ Chunk {i+1},{j+1} failed: {e}")

    if not all_features:
        print(f"⚠️ No geometries found in {name}")
        return None

    # Convert ESRI → GeoJSON
    features = []
    for feat in all_features:
        geom = feat.get("geometry")
        props = feat.get("attributes", {})
        if geom:
            try:
                gdf = gpd.GeoDataFrame.from_features(
                    [{"geometry": geom, "properties": props}], crs="EPSG:4326"
                )
                geojson = json.loads(gdf.to_json())
                features.append(geojson["features"][0])
            except Exception:
                pass

    if not features:
        print(f"⚠️ {name} contained no valid geometries.")
        return None

    geo = {"type": "FeatureCollection", "features": features}
    file_name = f"{name}.geojson"
    with open(file_name, "w", encoding="utf-8") as f:
        json.dump(geo, f)
    print(f"✅ Saved {file_name} ({len(features)} features)")
    return file_name

# ============================================================
# TILE BUILDER
# ============================================================
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

# ============================================================
# MAIN
# ============================================================
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
        print("\n❌ No tiles were built — check dataset endpoints or network.")

    print(f"\nTiles directory: {os.path.abspath(OUT_TILES_DIR)}")

if __name__ == "__main__":
    main()
