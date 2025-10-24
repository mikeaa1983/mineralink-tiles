#!/usr/bin/env python3
# mineralink_fetch_and_tile.py
# Fetches ArcGIS REST layers, reprojects to EPSG:4326, and builds Tippecanoe vector tiles.

import os
import json
import shutil
import subprocess
import requests
import geopandas as gpd
from pathlib import Path

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
DATASETS = [
    # --- WEST VIRGINIA ---
    {"name": "WV_wells", "url": "https://tagis.dep.wv.gov/arcgis/rest/services/WVDEP_enterprise/oil_gas/MapServer/1/query"},
    {"name": "WV_parcels", "url": "https://services.wvgis.wvu.edu/arcgis/rest/services/Planning_Cadastre/WV_Parcels/MapServer/0/query"},
    {"name": "WV_pipelines", "url": "https://tagis.dep.wv.gov/arcgis/rest/services/app_services/pipeline_construction/MapServer/0/query"},

    # --- OHIO ---
    {"name": "OH_wells", "url": "https://gis2.ohiodnr.gov/arcgis/rest/services/DOG_Services/Oilgas_Wells_10_JS_TEST/MapServer/0/query"},
    {"name": "OH_parcels", "url": "https://gis.ohiodnr.gov/arcgis/rest/services/OIT_Services/odnr_landbase/MapServer/4/query"},

    # --- PENNSYLVANIA ---
    {"name": "PA_wells", "url": "https://gis.dep.pa.gov/depgisprd/rest/services/OilGas_Collector/OG_Layers_OG_Well_Data/FeatureServer/0/query"},
    {"name": "PA_parcels", "url": "https://gis.dep.pa.gov/depgisprd/rest/services/Parcels/PA_Parcels/MapServer/0/query"},
    {"name": "PA_laterals", "url": "https://gis.dep.pa.gov/depgisprd/rest/services/OilGas_Collector/OG_Collector_Laterals/FeatureServer/0/query"},

    # --- TEXAS ---
    {"name": "TX_wells", "url": "https://www.gis.hctx.net/arcgishcpid/rest/services/TXRRC/Wells/MapServer/0/query"},
    {"name": "TX_parcels", "url": "https://feature.geographic.texas.gov/arcgis/rest/services/Parcels/stratmap24_land_parcels_48/MapServer/0/query"},
]

OUT_TILES_DIR = Path("tiles")
TIPPECANOE_CMD = "tippecanoe"
TIPPECANOE_MINZOOM = 4
TIPPECANOE_MAXZOOM = 14

# ------------------------------------------------------------
# FUNCTIONS
# ------------------------------------------------------------

def fetch_geojson(dataset):
    """Fetch a dataset as GeoJSON, with chunked logic for large layers like TX parcels."""
    name, url = dataset["name"], dataset["url"]
    print(f"\n=== Fetching {name} ===")

    params = {
        "where": "1=1",
        "outFields": "*",
        "f": "geojson",
        "outSR": "4326",
    }

    # Handle massive Texas parcels in smaller geographic chunks
    if name == "TX_parcels":
        # Roughly divides Texas into 10 bounding boxes (WGS84)
        bboxes = [
            (-106.7, 36.5, -104.5, 34.5),
            (-104.5, 36.5, -102.5, 34.5),
            (-102.5, 36.5, -100.5, 34.5),
            (-100.5, 36.5, -98.5, 34.5),
            (-98.5, 36.5, -96.5, 34.5),
            (-96.5, 36.5, -94.0, 34.5),
            (-106.7, 34.5, -104.5, 32.0),
            (-104.5, 34.5, -101.0, 32.0),
            (-101.0, 34.5, -97.5, 32.0),
            (-97.5, 34.5, -94.0, 32.0),
        ]
        features = []
        for i, (xmin, ymax, xmax, ymin) in enumerate(bboxes, start=1):
            print(f"  ▸ Chunk {i}/{len(bboxes)}: bbox {xmin},{ymin},{xmax},{ymax}")
            bbox_params = params.copy()
            bbox_params.update({
                "geometry": f"{xmin},{ymin},{xmax},{ymax}",
                "geometryType": "esriGeometryEnvelope",
                "spatialRel": "esriSpatialRelIntersects"
            })
            try:
                resp = requests.get(url, params=bbox_params, timeout=180)
                resp.raise_for_status()
                chunk = resp.json()
                feats = chunk.get("features", [])
                if feats:
                    features.extend(feats)
                    print(f"    + {len(feats)} features")
                else:
                    print(f"    (no features)")
            except Exception as e:
                print(f"⚠️  Chunk {i} failed: {e}")

        geo = {"type": "FeatureCollection", "features": features}
        file_name = f"{name}.geojson"
        with open(file_name, "w", encoding="utf-8") as f:
            json.dump(geo, f)
        print(f"✅ Combined {len(features)} features into {file_name}")
        return file_name

    # Default single-request fetch
    try:
        resp = requests.get(url, params=params, timeout=180)
        resp.raise_for_status()
        geo = resp.json()
        file_name = f"{name}.geojson"
        with open(file_name, "w", encoding="utf-8") as f:
            json.dump(geo, f)
        print(f"Saved {file_name} ({len(geo.get('features', []))} features)")
        return file_name
    except Exception as e:
        print(f"⚠️ Error fetching {name}: {e}")
        return None


def reproject_to_4326(input_file):
    """Ensure reprojection to EPSG:4326."""
    if not input_file or not os.path.exists(input_file):
        return None
    print(f"Reprojecting {input_file} → EPSG:4326")
    try:
        gdf = gpd.read_file(input_file)
        gdf = gdf.to_crs(epsg=4326)
        out_file = input_file.replace(".geojson", "_4326.geojson")
        gdf.to_file(out_file, driver="GeoJSON")
        print(f"Wrote {out_file}")
        return out_file
    except Exception as e:
        print(f"⚠️ Reprojection failed for {input_file}: {e}")
        return None


def build_tiles(name, geojson_file):
    """Run Tippecanoe to create vector tiles."""
    if not geojson_file or not os.path.exists(geojson_file):
        return
    out_dir = OUT_TILES_DIR / name
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        TIPPECANOE_CMD,
        "--output-to-directory", str(out_dir),
        "--layer", name,
        "--minimum-zoom", str(TIPPECANOE_MINZOOM),
        "--maximum-zoom", str(TIPPECANOE_MAXZOOM),
        "--force",
        geojson_file,
    ]
    print(f"Building tiles for {name} ...")
    subprocess.run(cmd, check=True)
    print(f"✅ Tiles built for {name} in {out_dir}")


def main():
    if OUT_TILES_DIR.exists():
        shutil.rmtree(OUT_TILES_DIR)
    OUT_TILES_DIR.mkdir(parents=True, exist_ok=True)

    for ds in DATASETS:
        src = fetch_geojson(ds)
        reproj = reproject_to_4326(src)
        build_tiles(ds["name"], reproj)
        if src and os.path.exists(src): os.remove(src)
        if reproj and os.path.exists(reproj): os.remove(reproj)

    print("\n🎉 All layers processed successfully! Tiles ready in /tiles/")


if __name__ == "__main__":
    main()
