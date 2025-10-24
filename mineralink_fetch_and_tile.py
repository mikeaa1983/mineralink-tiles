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
    {"name": "TX_parcels", "url": "https://feature.geographic.texas.gov/arcgis/rest/services/Parcels/stratmap25_land_parcels_48/MapServer/0/query"}
]


# ============================================================
# FUNCTIONS
# ============================================================
def fetch_geojson(dataset):
    """Fetch a dataset as GeoJSON."""
    name, url = dataset["name"], dataset["url"]
    print(f"\n=== Fetching {name} ===")

    params = {
        "where": "1=1",
        "outFields": "*",
        "f": "geojson",
        "outSR": "4326",
    }

    try:
        resp = requests.get(url, params=params, timeout=180)
        resp.raise_for_status()
        geo = resp.json()
        file_name = f"{name}.geojson"
        with open(file_name, "w", encoding="utf-8") as f:
            json.dump(geo, f)
        print(f"✅ Saved {file_name} ({len(geo.get('features', []))} features)")
        return file_name
    except Exception as e:
        print(f"⚠️ Error fetching {name}: {e}")
        return None


def reproject_to_4326(input_geojson):
    """Reproject to EPSG:4326 for Tippecanoe."""
    try:
        gdf = gpd.read_file(input_geojson)
        if gdf.empty:
            print(f"⚠️ {input_geojson} empty; skip reprojection")
            return None
        gdf = gdf.to_crs(epsg=4326)
        output_geojson = input_geojson.replace(".geojson", "_4326.geojson")
        gdf.to_file(output_geojson, driver="GeoJSON")
        print(f"🗺️ Wrote {output_geojson}")
        return output_geojson
    except Exception as e:
        print(f"⚠️ Reprojection error for {input_geojson}: {e}")
        return None


def build_tiles(name, geojson_file):
    """Run Tippecanoe to create vector tiles."""
    if not geojson_file or not os.path.exists(geojson_file):
        print(f"⚠️ Missing GeoJSON for {name}")
        return

    with open(geojson_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not data.get("features"):
        print(f"⚠️ {name} empty, skipping Tippecanoe.")
        return

    out_dir = os.path.join(OUT_TILES_DIR, name)
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
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
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Tippecanoe failed for {name}: {e}")


# ============================================================
# MAIN
# ============================================================
def main():
    print("=== Starting Mineralink Tile Builder ===")
    os.makedirs(OUT_TILES_DIR, exist_ok=True)

    for ds in DATASETS:
        f = fetch_geojson(ds)
        reproj = reproject_to_4326(f) if f else None
        build_tiles(ds["name"], reproj)

    print("\n🎉 Finished building all tiles!")


if __name__ == "__main__":
    main()
