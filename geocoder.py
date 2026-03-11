from __future__ import annotations

import argparse
import json
import math
import re
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from heapq import nlargest
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

from osgeo import ogr, osr

osr.UseExceptions()


ABBREVIATIONS = {
    "ch": "chemin",
    "av": "avenue",
    "rte": "route",
    "r": "rue",
    "pl": "place",
    "bd": "boulevard",
    "st": "saint",
    "ste": "sainte",
}


@dataclass(frozen=True)
class GazetteerRecord:
    idx: int
    address: str
    address_norm: str
    source_layer: str
    year: Optional[int]
    x: float
    y: float
    lon: float
    lat: float
    tokens: Tuple[str, ...]
    token_set: frozenset[str]
    trigrams: frozenset[str]
    house_number: Optional[str]


def normalize_text(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().replace("'", " ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    tokens = [ABBREVIATIONS.get(tok, tok) for tok in text.split()]
    return " ".join(tokens)


def extract_house_number(text_norm: str) -> Optional[str]:
    match = re.search(r"\b(\d+[a-z]?)\b", text_norm)
    return match.group(1) if match else None


def make_trigrams(text_norm: str) -> frozenset[str]:
    if not text_norm:
        return frozenset()
    padded = f"  {text_norm}  "
    return frozenset(padded[i : i + 3] for i in range(len(padded) - 2))


def levenshtein_similarity(a: str, b: str) -> float:
    if a == b:
        return 1.0
    if not a or not b:
        return 0.0

    if len(a) < len(b):
        a, b = b, a

    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        curr = [i]
        for j, cb in enumerate(b, start=1):
            cost = 0 if ca == cb else 1
            curr.append(
                min(
                    prev[j] + 1,
                    curr[j - 1] + 1,
                    prev[j - 1] + cost,
                )
            )
        prev = curr

    dist = prev[-1]
    max_len = max(len(a), len(b))
    return 1.0 - (dist / max_len)


def longest_common_substring_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0

    prev = [0] * (len(b) + 1)
    best = 0
    for i in range(1, len(a) + 1):
        curr = [0] * (len(b) + 1)
        ca = a[i - 1]
        for j in range(1, len(b) + 1):
            if ca == b[j - 1]:
                curr[j] = prev[j - 1] + 1
                if curr[j] > best:
                    best = curr[j]
        prev = curr

    return best / max(len(a), len(b))


def year_similarity(query_year: int, record_year: Optional[int], sigma: float = 40.0) -> float:
    if record_year is None:
        return 0.0
    delta = abs(query_year - record_year)
    return math.exp(-((delta * delta) / (2.0 * sigma * sigma)))


def _build_transform(src_epsg: int, dst_epsg: int) -> osr.CoordinateTransformation:
    src = osr.SpatialReference()
    src.ImportFromEPSG(src_epsg)
    src.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    dst = osr.SpatialReference()
    dst.ImportFromEPSG(dst_epsg)
    dst.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    return osr.CoordinateTransformation(src, dst)


def _infer_year_from_source_layer(source_layer: str) -> Optional[int]:
    match = re.search(r"(1[6-9]\d{2}|20\d{2})", source_layer)
    return int(match.group(1)) if match else None


class GazetteerIndex:
    def __init__(self, records: Sequence[GazetteerRecord]):
        self.records = list(records)
        self._records_by_id = {rec.idx: rec for rec in self.records}
        self._token_index: Dict[str, Set[int]] = defaultdict(set)
        self._trigram_index: Dict[str, Set[int]] = defaultdict(set)
        self._token_idf: Dict[str, float] = {}
        self._trigram_idf: Dict[str, float] = {}
        self._build_indices()

    @classmethod
    def from_gpkg(
        cls,
        gpkg_path: str | Path,
        layer_name: str = "gazetteer",
        target_epsg: int = 2056,
    ) -> "GazetteerIndex":
        ds = ogr.Open(str(gpkg_path))
        if ds is None:
            raise FileNotFoundError(f"Could not open GeoPackage: {gpkg_path}")

        layer = ds.GetLayerByName(layer_name)
        if layer is None:
            raise ValueError(f"Layer '{layer_name}' not found in {gpkg_path}")

        source_srs = layer.GetSpatialRef()
        to_target = None
        if source_srs is not None:
            source_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
            target_srs = osr.SpatialReference()
            target_srs.ImportFromEPSG(target_epsg)
            target_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
            if not source_srs.IsSame(target_srs):
                to_target = osr.CoordinateTransformation(source_srs, target_srs)

        to_wgs84 = _build_transform(target_epsg, 4326)
        records: List[GazetteerRecord] = []

        for feat in layer:
            props = feat.items()
            raw_address = props.get("address")
            if raw_address is None:
                continue
            address = str(raw_address).strip()
            if not address:
                continue

            source_layer = str(props.get("source_layer") or "unknown")
            year_value = props.get("year")
            if year_value is None or str(year_value).strip() == "":
                year = _infer_year_from_source_layer(source_layer)
            else:
                try:
                    year = int(year_value)
                except (ValueError, TypeError):
                    year = _infer_year_from_source_layer(source_layer)

            geom = feat.GetGeometryRef()
            if geom is None:
                continue
            geom = geom.Clone()

            if to_target is not None:
                geom.Transform(to_target)

            if geom.GetGeometryType() not in (ogr.wkbPoint, ogr.wkbPoint25D):
                geom = geom.Centroid()

            x, y, *_ = geom.GetPoint()
            ll = geom.Clone()
            ll.Transform(to_wgs84)
            lon, lat, *_ = ll.GetPoint()

            address_norm = normalize_text(address)
            tokens = tuple(address_norm.split())
            token_set = frozenset(tokens)
            trigrams = make_trigrams(address_norm)
            number = extract_house_number(address_norm)

            records.append(
                GazetteerRecord(
                    idx=len(records),
                    address=address,
                    address_norm=address_norm,
                    source_layer=source_layer,
                    year=year,
                    x=x,
                    y=y,
                    lon=lon,
                    lat=lat,
                    tokens=tokens,
                    token_set=token_set,
                    trigrams=trigrams,
                    house_number=number,
                )
            )

        return cls(records)

    @classmethod
    def from_geojson_dir(
        cls,
        data_dir: str | Path,
        pattern: str = "*.geojson",
        target_epsg: int = 2056,
    ) -> "GazetteerIndex":
        data_path = Path(data_dir)
        files = sorted(data_path.glob(pattern))
        if not files:
            raise FileNotFoundError(f"No files found in {data_path} with pattern {pattern}")

        to_wgs84 = _build_transform(target_epsg, 4326)
        records: List[GazetteerRecord] = []

        for geojson_path in files:
            ds = ogr.Open(str(geojson_path))
            if ds is None:
                continue

            layer = ds.GetLayer(0)
            if layer is None:
                continue

            source_layer = geojson_path.stem
            year = _infer_year_from_source_layer(source_layer)

            layer_srs = layer.GetSpatialRef()
            to_target = None
            if layer_srs is not None:
                layer_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
                target_srs = osr.SpatialReference()
                target_srs.ImportFromEPSG(target_epsg)
                target_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
                if not layer_srs.IsSame(target_srs):
                    to_target = osr.CoordinateTransformation(layer_srs, target_srs)

            for feat in layer:
                props = feat.items()
                raw_address = props.get("address")
                if raw_address is None:
                    continue
                address = str(raw_address).strip()
                if not address:
                    continue

                geom = feat.GetGeometryRef()
                if geom is None:
                    continue
                geom = geom.Clone()

                if to_target is not None:
                    geom.Transform(to_target)

                if geom.GetGeometryType() not in (ogr.wkbPoint, ogr.wkbPoint25D):
                    geom = geom.Centroid()

                x, y, *_ = geom.GetPoint()

                ll = geom.Clone()
                ll.Transform(to_wgs84)
                lon, lat, *_ = ll.GetPoint()

                address_norm = normalize_text(address)
                tokens = tuple(address_norm.split())
                token_set = frozenset(tokens)
                trigrams = make_trigrams(address_norm)
                number = extract_house_number(address_norm)

                records.append(
                    GazetteerRecord(
                        idx=len(records),
                        address=address,
                        address_norm=address_norm,
                        source_layer=source_layer,
                        year=year,
                        x=x,
                        y=y,
                        lon=lon,
                        lat=lat,
                        tokens=tokens,
                        token_set=token_set,
                        trigrams=trigrams,
                        house_number=number,
                    )
                )

        return cls(records)

    def _build_indices(self) -> None:
        n = len(self.records)
        if n == 0:
            return

        for rec in self.records:
            for tok in rec.token_set:
                self._token_index[tok].add(rec.idx)
            for tri in rec.trigrams:
                self._trigram_index[tri].add(rec.idx)

        self._token_idf = {
            tok: math.log((n + 1.0) / (len(doc_ids) + 1.0)) + 1.0
            for tok, doc_ids in self._token_index.items()
        }
        self._trigram_idf = {
            tri: math.log((n + 1.0) / (len(doc_ids) + 1.0)) + 1.0
            for tri, doc_ids in self._trigram_index.items()
        }

    def export_for_elasticsearch(self) -> List[Dict[str, object]]:
        docs: List[Dict[str, object]] = []
        for rec in self.records:
            docs.append(
                {
                    "id": str(rec.idx),
                    "address": rec.address,
                    "address_norm": rec.address_norm,
                    "source_layer": rec.source_layer,
                    "year": rec.year,
                    "x": rec.x,
                    "y": rec.y,
                    "location": {"lat": rec.lat, "lon": rec.lon},
                }
            )
        return docs

    def _candidate_ids(self, query_norm: str, limit: int = 1500) -> List[int]:
        q_tokens = set(query_norm.split())
        q_trigrams = make_trigrams(query_norm)

        candidate_scores: Dict[int, float] = defaultdict(float)

        for tok in q_tokens:
            for idx in self._token_index.get(tok, ()):  # exact token hits
                candidate_scores[idx] += 2.5 * self._token_idf.get(tok, 1.0)

        for tri in q_trigrams:
            for idx in self._trigram_index.get(tri, ()):  # fuzzy trigram overlap
                candidate_scores[idx] += 0.6 * self._trigram_idf.get(tri, 1.0)

        if not candidate_scores:
            return list(range(len(self.records)))

        return [idx for idx, _ in nlargest(limit, candidate_scores.items(), key=lambda kv: kv[1])]

    def search(
        self,
        query: str,
        top_k: int = 10,
        year: Optional[int] = None,
        year_weight: float = 0.2,
        candidate_ids: Optional[Sequence[int]] = None,
        candidate_external_scores: Optional[Dict[int, float]] = None,
        external_score_weight: float = 0.2,
    ) -> List[Dict[str, object]]:
        query_norm = normalize_text(query)
        if not query_norm:
            return []

        q_house_number = extract_house_number(query_norm)

        stage1_ids = (
            [idx for idx in candidate_ids if idx in self._records_by_id]
            if candidate_ids is not None
            else self._candidate_ids(query_norm, limit=2000)
        )

        stage2: List[Tuple[int, float, float]] = []
        for idx in stage1_ids:
            rec = self._records_by_id[idx]
            lcs = longest_common_substring_similarity(query_norm, rec.address_norm)
            fast_score = lcs
            stage2.append((idx, fast_score, lcs))

        stage2.sort(key=lambda row: row[1], reverse=True)
        stage2 = stage2[:400]

        ranked: List[Tuple[float, float, float, GazetteerRecord]] = []
        for idx, _, lcs in stage2:
            rec = self._records_by_id[idx]
            lev = levenshtein_similarity(query_norm, rec.address_norm)

            text_score = 0.7 * lev + 0.3 * lcs

            if q_house_number and rec.house_number:
                if q_house_number == rec.house_number:
                    text_score += 0.05

            text_score = min(1.0, text_score)

            if year is None:
                temporal_score = text_score
            else:
                ysim = year_similarity(year, rec.year)
                temporal_score = (1.0 - year_weight) * text_score + year_weight * ysim

            external_score = 0.0
            if candidate_external_scores is not None:
                external_score = max(0.0, min(1.0, candidate_external_scores.get(idx, 0.0)))
                final_score = (1.0 - external_score_weight) * temporal_score + external_score_weight * external_score
            else:
                final_score = temporal_score

            ranked.append((final_score, text_score, external_score, rec))

        ranked.sort(key=lambda row: row[0], reverse=True)

        out: List[Dict[str, object]] = []
        for final_score, text_score, external_score, rec in ranked[:top_k]:
            out.append(
                {
                    "address": rec.address,
                    "latitude": rec.lat,
                    "longitude": rec.lon,
                    "score": round(final_score, 6),
                    "text_score": round(text_score, 6),
                    "retrieval_score": round(external_score, 6),
                    "source_layer": rec.source_layer,
                    "year": rec.year,
                }
            )

        return out


class ElasticGazetteerBackend:
    def __init__(self, base_url: str, index_name: str):
        self.base_url = base_url.rstrip("/")
        self.index_name = index_name

    def _request_json(
        self,
        method: str,
        path: str,
        payload: Optional[dict] = None,
        timeout: float = 3.0,
        headers: Optional[Dict[str, str]] = None,
    ) -> dict:
        url = f"{self.base_url}{path}"
        body = None
        req_headers = {"Accept": "application/json"}
        if headers:
            req_headers.update(headers)

        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            req_headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url, method=method, data=body, headers=req_headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
        return json.loads(raw.decode("utf-8")) if raw else {}

    def _request_raw(
        self,
        method: str,
        path: str,
        payload: Optional[str] = None,
        timeout: float = 20.0,
        content_type: str = "application/x-ndjson",
    ) -> dict:
        url = f"{self.base_url}{path}"
        body = payload.encode("utf-8") if payload is not None else None
        headers = {"Accept": "application/json", "Content-Type": content_type}
        req = urllib.request.Request(url, method=method, data=body, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
        return json.loads(raw.decode("utf-8")) if raw else {}

    def ping(self) -> bool:
        try:
            self._request_json("GET", "/", timeout=1.2)
            return True
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError):
            return False

    def index_exists(self) -> bool:
        url = f"{self.base_url}/{self.index_name}"
        req = urllib.request.Request(url, method="HEAD")
        try:
            with urllib.request.urlopen(req, timeout=1.5):
                return True
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return False
            raise

    def create_index(self, overwrite: bool = False) -> None:
        if overwrite and self.index_exists():
            self._request_json("DELETE", f"/{self.index_name}")

        if self.index_exists():
            return

        mapping = {
            "settings": {
                "analysis": {
                    "analyzer": {
                        "fr_folded": {
                            "tokenizer": "standard",
                            "filter": ["lowercase", "asciifolding"],
                        },
                        "fr_edge": {
                            "tokenizer": "edge_tok",
                            "filter": ["lowercase", "asciifolding"],
                        },
                    },
                    "tokenizer": {
                        "edge_tok": {
                            "type": "edge_ngram",
                            "min_gram": 2,
                            "max_gram": 20,
                            "token_chars": ["letter", "digit"],
                        }
                    },
                }
            },
            "mappings": {
                "properties": {
                    "address": {
                        "type": "text",
                        "analyzer": "fr_folded",
                        "fields": {
                            "keyword": {"type": "keyword"},
                            "ngram": {"type": "text", "analyzer": "fr_edge"},
                        },
                    },
                    "address_norm": {
                        "type": "text",
                        "analyzer": "fr_folded",
                        "fields": {
                            "ngram": {"type": "text", "analyzer": "fr_edge"},
                        },
                    },
                    "source_layer": {"type": "keyword"},
                    "year": {"type": "integer"},
                    "location": {"type": "geo_point"},
                    "x": {"type": "double"},
                    "y": {"type": "double"},
                }
            },
        }
        self._request_json("PUT", f"/{self.index_name}", payload=mapping, timeout=8.0)

    def bulk_index(self, docs: Sequence[Dict[str, object]], batch_size: int = 2000) -> None:
        if not docs:
            return

        for start in range(0, len(docs), batch_size):
            batch = docs[start : start + batch_size]
            lines: List[str] = []
            for doc in batch:
                action = {"index": {"_index": self.index_name, "_id": doc["id"]}}
                lines.append(json.dumps(action, ensure_ascii=False))
                source = {k: v for k, v in doc.items() if k != "id"}
                lines.append(json.dumps(source, ensure_ascii=False))

            payload = "\n".join(lines) + "\n"
            res = self._request_raw("POST", "/_bulk?refresh=false", payload=payload, timeout=30.0)
            if res.get("errors"):
                raise RuntimeError("Elasticsearch bulk indexing returned errors")

        self._request_json("POST", f"/{self.index_name}/_refresh")

    def search_candidates(self, query: str, query_norm: str, size: int = 1500) -> List[Tuple[int, float]]:
        body = {
            "size": size,
            "_source": False,
            "query": {
                "bool": {
                    "should": [
                        {"match_phrase": {"address": {"query": query, "slop": 2, "boost": 6.0}}},
                        {"match": {"address": {"query": query, "boost": 4.0}}},
                        {
                            "match": {
                                "address": {
                                    "query": query,
                                    "fuzziness": "AUTO",
                                    "prefix_length": 1,
                                    "max_expansions": 100,
                                    "boost": 3.8,
                                }
                            }
                        },
                        {"match": {"address.ngram": {"query": query, "boost": 2.2}}},
                        {"match": {"address_norm": {"query": query_norm, "fuzziness": "AUTO", "boost": 3.0}}},
                        {"match": {"address_norm.ngram": {"query": query_norm, "boost": 1.8}}},
                    ],
                    "minimum_should_match": 1,
                }
            },
        }

        res = self._request_json("POST", f"/{self.index_name}/_search", payload=body, timeout=6.0)
        hits = res.get("hits", {}).get("hits", [])
        if not hits:
            return []

        max_score = hits[0].get("_score") or 1.0
        candidates: List[Tuple[int, float]] = []

        for hit in hits:
            doc_id = hit.get("_id")
            if doc_id is None:
                continue
            try:
                idx = int(doc_id)
            except ValueError:
                continue

            score = float(hit.get("_score") or 0.0)
            norm_score = max(0.0, min(1.0, score / max_score))
            candidates.append((idx, norm_score))

        return candidates


_DEFAULT_INDEX: Optional[GazetteerIndex] = None


def _get_default_index(data_dir: str | Path = "DATA") -> GazetteerIndex:
    global _DEFAULT_INDEX
    if _DEFAULT_INDEX is None:
        data_path = Path(data_dir)
        gpkg_path = data_path / "gazetteer_merged.gpkg"
        if gpkg_path.exists():
            _DEFAULT_INDEX = GazetteerIndex.from_gpkg(gpkg_path)
        else:
            _DEFAULT_INDEX = GazetteerIndex.from_geojson_dir(data_path)
    return _DEFAULT_INDEX


def setup_elasticsearch_index(
    data_dir: str | Path = "DATA",
    es_url: str = "http://localhost:9200",
    es_index: str = "lausanne_gazetteer",
    overwrite: bool = False,
) -> Dict[str, object]:
    """Create and populate an Elasticsearch gazetteer index from local data."""

    idx = _get_default_index(data_dir)
    es = ElasticGazetteerBackend(base_url=es_url, index_name=es_index)

    if not es.ping():
        raise ConnectionError(f"Elasticsearch not reachable at {es_url}")

    es.create_index(overwrite=overwrite)
    es.bulk_index(idx.export_for_elasticsearch())

    return {
        "index": es_index,
        "documents": len(idx.records),
        "es_url": es_url,
        "overwrite": overwrite,
    }


def search_gazetteer(
    query: str,
    year: Optional[int] = None,
    top_k: int = 10,
    data_dir: str | Path = "DATA",
    use_elasticsearch: bool = True,
    es_url: str = "http://localhost:9200",
    es_index: str = "lausanne_gazetteer",
    es_candidate_k: int = 1500,
    auto_setup_es: bool = False,
) -> List[Dict[str, object]]:
    """Hybrid retrieval for historical geocoding.

    Stage 1: Elasticsearch candidate retrieval (BM25 + fuzziness + edge n-grams).
    Stage 2: local fuzzy reranking (Levenshtein + longest-common-substring).
    Stage 3: optional year-aware score adjustment.

    Returns ranked hits with address, latitude, longitude, and scores.
    """

    index = _get_default_index(data_dir)

    if use_elasticsearch:
        es = ElasticGazetteerBackend(base_url=es_url, index_name=es_index)
        try:
            if es.ping():
                if not es.index_exists():
                    if auto_setup_es:
                        es.create_index(overwrite=False)
                        es.bulk_index(index.export_for_elasticsearch())
                    else:
                        return index.search(query=query, year=year, top_k=top_k)

                query_norm = normalize_text(query)
                candidates = es.search_candidates(query=query, query_norm=query_norm, size=es_candidate_k)
                if candidates:
                    candidate_ids = [idx for idx, _ in candidates]
                    candidate_scores = {idx: score for idx, score in candidates}
                    return index.search(
                        query=query,
                        year=year,
                        top_k=top_k,
                        candidate_ids=candidate_ids,
                        candidate_external_scores=candidate_scores,
                        external_score_weight=0.2,
                    )
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, RuntimeError):
            pass

    return index.search(query=query, year=year, top_k=top_k)


def main() -> None:
    parser = argparse.ArgumentParser(description="Hybrid gazetteer geocoder")
    parser.add_argument("query", nargs="?", type=str, help="Address or place-name query")
    parser.add_argument("--year", type=int, default=None, help="Optional target year")
    parser.add_argument("--top-k", type=int, default=10, help="Number of results")
    parser.add_argument("--data-dir", type=str, default="DATA", help="Directory with GeoJSON layers")

    parser.add_argument("--es-url", type=str, default="http://localhost:9200", help="Elasticsearch URL")
    parser.add_argument("--es-index", type=str, default="lausanne_gazetteer", help="Elasticsearch index name")
    parser.add_argument("--no-es", action="store_true", help="Disable Elasticsearch stage")

    parser.add_argument("--setup-es", action="store_true", help="Create and populate Elasticsearch index")
    parser.add_argument("--overwrite-es", action="store_true", help="Recreate Elasticsearch index")
    args = parser.parse_args()

    if args.setup_es:
        info = setup_elasticsearch_index(
            data_dir=args.data_dir,
            es_url=args.es_url,
            es_index=args.es_index,
            overwrite=args.overwrite_es,
        )
        print(
            f"Indexed {info['documents']} documents into {info['index']} at {info['es_url']} "
            f"(overwrite={info['overwrite']})"
        )
        if not args.query:
            return

    if not args.query:
        parser.error("query is required unless using --setup-es only")

    rows = search_gazetteer(
        query=args.query,
        year=args.year,
        top_k=args.top_k,
        data_dir=args.data_dir,
        use_elasticsearch=not args.no_es,
        es_url=args.es_url,
        es_index=args.es_index,
    )

    for i, row in enumerate(rows, start=1):
        print(
            f"{i:02d}. score={row['score']:.4f} | "
            f"{row['address']} | "
            f"{row['latitude']:.6f}, {row['longitude']:.6f} | "
            f"{row['source_layer']}"
        )


if __name__ == "__main__":
    main()
