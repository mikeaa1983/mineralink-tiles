#!/usr/bin/env python3
import os, json, requests, subprocess, shutil, sys
import geopandas as gpd
from shapely.geometry import shape

# ============================================================
# CONFIGURATION
# ============================================================
OUT_TILES_DIR = "tiles"
TIPPECANOE_CMD = "tippecanoe"
TIPPECANOE_MINZOOM = 4
TIPPECANOE_MAXZOOM = 14

DATASETS = [
    # --- WEST VIRGINIA ---
    {"name": "WV_wells", "url": "https://tagis.dep.wv.gov/arcgis/rest/services/WVDEP_enterprise/oil_gas/MapServer/0/query"},
    {"name": "WV_parcels", "url": "https://services.wvgis.wvu.edu/arcgis/rest/services/Planning_Cadastre/WV_Parcels/MapServer/0/query"},
    {"name": "WV_pipelines", "url": "https://tagis.dep.wv.gov/arcgis/rest/services/app_services/pipeline_construction/MapServer/0/query"},

    # --- OHIO ---
    {"name": "OH_wells", "url": "https://gis.ohiodnr.gov/arcgis/rest/services/DOG_Services/MapServer/0/query"},
    {"name": "OH_parcels", "url": "https://gis.ohiodnr.gov/arcgis/rest/services/OIT_Services/odnr_landbase/MapServer/4/query"},

    # --- PENNSYLVANIA ---
    {"name": "PA_wells", "url": "https://gis.dep.pa.gov/depgisprd/rest/services/OilGas_Collector/OG_Layers_OG_Well_Data/FeatureServer/0/query"},
    {"name": "PA_parcels", "url": "https://gis.dep.pa.gov/depgisprd/rest/services/Parcels/PA_Parcels/MapServer/0/query"},
    {"name": "PA_laterals", "url": "https://gis.dep.pa.gov/depgisprd/rest/services/OilGas_Collector/OG_Collector_Laterals/FeatureServer/0/query"},

    # --- TEXAS ---
    {"name": "TX_wells", "url": "https://gis.rrc.texas.gov/arcgis/rest/services/RRC_Public/RRC_Wells/MapServer/0/query"},
    {"name": "TX_parcels", "url": "https://feature.geographic.texas.gov/arcgis/rest/services/Parcels/stratmap25_land_parcels_48/MapServer/0/query"},
]


# ============================================================
# FETCH + CONVERT FUNCTIONS
# ============================================================
def fetch_geojson(dataset):
    """Fetch a dataset and convert ESRI JSON → GeoJSON if needed."""
    name, url = dataset["name"], dataset["url"]
    print(f"\n=== Fetching {name} ===")

    params = {
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "true",
        "f": "json",
        "outSR": "4326",
    }

    try:
        resp = requests.get(url, params=params, timeout=180)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"⚠️ Failed to fetch {name}: {e}")
        return None

    # Convert ESRI JSON → GeoJSON
    try:
        if "features" not in data:
            print(f"⚠️ {name} returned no features")
            return None

        features = []
        for feat in data["features"]:
            geom = feat.get("geometry")
            if not geom:
                continue
            props = feat.get("attributes", {})
            try:
                # ESRI geometries can be converted with geopandas
                gdf = gpd.GeoDataFrame.from_features([{"geometry": geom, "properties": props}], crs="EPSG:4326")
                geojson = json.loads(gdf.to_json())
                features.append(geojson["features"][0])
            except Exception as e:
                pass

        if not features:
            print(f"⚠️ No geometries found in {name}")
            return None

        geo = {"type": "FeatureCollection", "features": features}
        file_name = f"{name}.geojson"
        with open(file_name, "w", encoding="utf-8") as f:
            json.dump(geo, f)
        print(f"✅ Saved {file_name} ({len(features)} features)")
        return file_name

    except Exception as e:
        print(f"⚠️ Error converting {name}: {e}")
        return None


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
