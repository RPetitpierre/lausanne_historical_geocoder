#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob

DATA_DIR="${1:-DATA}"
OUT_GPKG="${2:-$DATA_DIR/gazetteer_merged.gpkg}"
LAYER_NAME="${3:-gazetteer}"
AUTOMATED_DIR="$DATA_DIR/automated_extraction"

if ! command -v ogr2ogr >/dev/null 2>&1; then
  echo "Error: ogr2ogr is required but not found in PATH." >&2
  exit 1
fi

files=("$DATA_DIR"/*.geojson "$AUTOMATED_DIR"/*.geojson)
if [ "${#files[@]}" -eq 0 ]; then
  echo "Error: no GeoJSON files found in $DATA_DIR or $AUTOMATED_DIR" >&2
  exit 1
fi

rm -f "$OUT_GPKG"
first=1

for src in "${files[@]}"; do
  src_layer="$(basename "$src" .geojson)"
  src_table="$(ogrinfo -ro -so "$src" 2>/dev/null | sed -nE 's/^1: ([^ ]+) .*/\1/p' | head -n 1)"
  if [ -z "$src_table" ]; then
    echo "Error: could not determine layer name for $src" >&2
    exit 1
  fi
  src_year="$(echo "$src_layer" | sed -nE 's/.*(1[6-9][0-9]{2}|20[0-9]{2}).*/\1/p')"
  if [ -z "$src_year" ]; then
    year_sql="NULL"
  else
    year_sql="$src_year"
  fi

  address_field="address"
  geometry_expr="geometry"
  if [[ "$src" == "$AUTOMATED_DIR"/* ]]; then
    address_field="label"
    geometry_expr="ST_Centroid(geometry)"
  fi

  sql="SELECT $geometry_expr AS geometry, '$src_layer' AS source_layer, \"$address_field\" AS address, $year_sql AS year FROM \"$src_table\" WHERE \"$address_field\" IS NOT NULL AND TRIM(\"$address_field\") <> ''"

  if [ "$first" -eq 1 ]; then
    ogr2ogr \
      -f GPKG "$OUT_GPKG" "$src" \
      -nln "$LAYER_NAME" \
      -nlt POINT \
      -t_srs EPSG:2056 \
      -dialect SQLITE \
      -sql "$sql"
    first=0
  else
    ogr2ogr \
      -f GPKG "$OUT_GPKG" "$src" \
      -update -append \
      -nln "$LAYER_NAME" \
      -nlt POINT \
      -t_srs EPSG:2056 \
      -dialect SQLITE \
      -sql "$sql"
  fi
done

echo "Created: $OUT_GPKG"
ogrinfo -so "$OUT_GPKG" "$LAYER_NAME" | sed -n '1,40p'
