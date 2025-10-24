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
    {"name": "TX_wells", "url": "https://rrc-txdigital.maps.arcgis.com/sharing/rest/content/items/5a28b3085edb47bfa8f35e6d8a3124b8/data"},
    {"name": "TX_parcels", "url": "https://feature.geographic.texas.gov/arcgis/rest/services/Parcels/stratmap25_land_parcels_48/MapServer/0/query"},
]

# ============================================================
# FUNCTIONS
# ============================================================

def fetch_geojson(dataset):
    """Fetch dataset as GeoJSON, chunking Texas parcels."""
    name, url = dataset["name"], dataset["url"]
    print(f"\n=== Fetching {name} ===")

    params = {
        "where": "1=1",
        "outFields": "*",
        "f": "geojson",
        "outSR": "4326"
    }

    # --- Chunk TX parcels ---
    if name == "TX_parcels":
        bboxes = [
            (-106.7, 34.5, -94.0, 36.5),
            (-106.7, 32.0, -94.0, 34.5),
            (-106.7, 29.5, -94.0, 32.0)
        ]
        features = []
        for i, (xmin, ymin, xmax, ymax) in enumerate(bboxes, start=1):
            print(f" ▸ Chunk {i}/{len(bboxes)}: {xmin},{ymin},{xmax},{ymax}")
            bbox_params = params.copy()
            bbox_params.update({
                "geometry": f"{xmin},{ymin},{xmax},{ymax}",
                "geometryType": "esriGeometryEnvelope",
                "spatialRel": "esriSpatialRelIntersects"
            })
            try:
                r = requests.get(url, params=bbox_params, timeout=180)
                r.raise_for_status()
                chunk = r.json()
                feats = chunk.get("features", [])
                features.extend(feats)
                print(f"   + {len(feats)} features")
            except Exception as e:
                print(f"⚠️ Chunk {i} failed: {e}")

        geo = {"type": "FeatureCollection", "features": features}
        out_file = f"{name}.geojson"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(geo, f)
        print(f"✅ Combined {len(features)} features -> {out_file}")
        return out_file

    # --- Normal datasets ---
    try:
        r = requests.get(url, params=params, timeout=180)
        r.raise_for_status()
        geo = r.json()
        out_file = f"{name}.geojson"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(geo, f)
        print(f"Saved {out_file} ({len(geo.get('features', []))} features)")
        return out_file
    except Exception as e:
        print(f"⚠️ Error fetching {name}: {e}")
        return None


def reproject_to_4326(in_geojson):
    """Force to EPSG:4326 if CRS missing."""
    try:
        gdf = gpd.read_file(in_geojson)
        if gdf.empty:
            print(f"⚠️ {in_geojson} is empty, skipping reprojection.")
            return None
        if gdf.crs is None:
            gdf.set_crs(epsg=3857, inplace=True)
        gdf = gdf.to_crs(epsg=4326)
        out_geojson = in_geojson.replace(".geojson", "_4326.geojson")
        gdf.to_file(out_geojson, driver="GeoJSON")
        print(f"Reprojected -> {out_geojson}")
        return out_geojson
    except Exception as e:
        print(f"⚠️ Error reprojecting {in_geojson}: {e}")
        return None


def build_tiles(name, geojson_file):
    """Run Tippecanoe and build full zoom vector tiles."""
    if not geojson_file or not os.path.exists(geojson_file):
        print(f"⚠️ No GeoJSON for {name}, skipping.")
        return

    try:
        with open(geojson_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not data.get("features"):
            print(f"⚠️ {name} has no features, skipping Tippecanoe.")
            return
    except Exception as e:
        print(f"⚠️ Could not read {geojson_file}: {e}")
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
        "--no-feature-limit",
        "--no-tile-size-limit",
        "--force",
        geojson_file
    ]
    print(f"Building tiles for {name} ...")
    try:
        subprocess.run(cmd, check=True)
        print(f"✅ Tiles built for {name} -> {out_dir}")
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
        repro = reproject_to_4326(f) if f else None
        build_tiles(ds["name"], repro)

    print("\n🎉 All layers processed successfully! Tiles ready in /tiles/")

if __name__ == "__main__":
    main()
