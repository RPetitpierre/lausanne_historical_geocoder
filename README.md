# Lausanne Gazetteer & Historical Geocoder

Historical gazetteer and geocoder for Lausanne (1722 to present), built from multiple historical and modern map layers.

The merged database is `DATA/gazetteer_merged.gpkg`, with point geometries (EPSG:2056) and fields:
- `address`
- `year`
- `source_layer`

## Sources

Source files used by `build_gazetteer_db.sh`:
- `DATA/*.geojson` (native point layers, `address` field)
- `DATA/automated_extraction/*.geojson` (multipolygon layers, `label` field mapped to `address`, centroid used for point conversion)

Current merged entry count: **36,382**

| Source layer | Entries |
|---|---:|
| 1722_melotte | 138 |
| 1723_melotte_ensemble | 2,633 |
| 1806_emery | 302 |
| 1824_spengler | 248 |
| 1831_berney | 749 |
| 1836_projet_ceinture_Pichard | 251 |
| 1838_berney_ensemble | 281 |
| 1852_zollikofer | 244 |
| 1854_plan_Weber | 761 |
| 1856_spengler | 203 |
| 1858_weber | 430 |
| 1865_siegfried | 1,269 |
| 1871_sprengler | 408 |
| 1871_sprengler_profil | 295 |
| 1875_decrousaz | 436 |
| 1900_payot | 997 |
| 1903_reber | 484 |
| 1925_societe_des_transports | 646 |
| 1959_cadastre_montchoisi | 177 |
| 1970_imhoff | 516 |
| 2023_cadastre_addresses | 10,847 |
| 2023_cadastre_addresses_short | 10,847 |
| 2023_cadastre_lieux | 1,610 |
| 2023_cadastre_lieux_short | 1,610 |

## Geocoding Algorithm

The geocoder uses a two-stage (three-stage with Elasticsearch) ranking pipeline:

1. Candidate retrieval
- Local mode: token + trigram inverted indices.
- Hybrid mode: Elasticsearch BM25/fuzzy/edge-ngram retrieval.

2. Text reranking (local fuzzy scoring)
- `lev = levenshtein_similarity(query_norm, address_norm)`
- `lcs = longest_common_substring_similarity(query_norm, address_norm)`
- `text_score = 0.7 * lev + 0.3 * lcs`
- If house numbers match exactly, `text_score += 0.05`
- Then `text_score` is capped to `1.0`

3. Temporal + retrieval blending
- Year similarity (if year provided):
  - `year_similarity = exp(-(delta^2) / (2 * sigma^2))`, with `sigma = 40`
  - `temporal_score = 0.8 * text_score + 0.2 * year_similarity`
- If Elasticsearch retrieval score exists:
  - `final_score = 0.8 * temporal_score + 0.2 * retrieval_score` (or `text_score` instead of `temporal_score` when no year is provided)

## Usage

Prerequisites:
- `ogr2ogr` (GDAL) available in `PATH`
- Python 3 with `osgeo` bindings

Build the merged gazetteer database:

```bash
./build_gazetteer_db.sh
```

This creates `DATA/gazetteer_merged.gpkg` with fields:
`source_layer`, `address`, `year`, and point geometry (EPSG:2056).

Run a query (local fuzzy ranking):

```bash
python3 geocoder.py "Pre d Ouchy" --year 1831 --top-k 5 --no-es
```

Optional: setup Elasticsearch + run hybrid search:

```bash
python3 geocoder.py --setup-es --overwrite-es
python3 geocoder.py "Pre d Ouchy" --year 1831 --top-k 5
```

Python API:

```python
from geocoder import search_gazetteer
rows = search_gazetteer("Pre d Ouchy", year=1831, top_k=5)
```

Each result includes: `address`, `latitude`, `longitude`, `score`, `source_layer`, `year`.
