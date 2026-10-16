"""
SciPulse — Accès au data lake MinIO (S3)

Toutes les couches Bronze et Silver passent par ce module :
  - JSON Lines pour le Bronze (fidèle à la source, lisible ligne à ligne)
  - Parquet pour le Silver (typé, colonnaire, lu nativement par Spark)

Le client boto3 est créé à la demande (get_s3), ce qui permet aux tests de le
remplacer par un faux S3 (moto) sans serveur MinIO.
"""

import io
import json
from datetime import datetime, timezone
from functools import lru_cache
from typing import Iterable, Iterator

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from src.config import ALL_BUCKETS, MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY


@lru_cache(maxsize=1)
def get_s3():
    """Client S3 pointant sur MinIO (signature v4, adressage path-style)."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )


def ensure_buckets(buckets: Iterable[str] = ALL_BUCKETS):
    """Crée les buckets absents (normalement fait par le conteneur minio-init)."""
    s3 = get_s3()
    for bucket in buckets:
        try:
            s3.head_bucket(Bucket=bucket)
        except ClientError:
            s3.create_bucket(Bucket=bucket)


# ── Listage ──────────────────────────────────────────────────────────────────

def list_objects(bucket: str, prefix: str = "") -> list[dict]:
    """Objets d'un préfixe : [{Key, Size, LastModified}], triés par clé. Ignore les « _state/ »."""
    paginator = get_s3().get_paginator("list_objects_v2")
    objects = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if "/_state/" in f"/{obj['Key']}":
                continue
            objects.append({"Key": obj["Key"], "Size": obj["Size"], "LastModified": obj["LastModified"]})
    return sorted(objects, key=lambda o: o["Key"])


def list_keys(bucket: str, prefix: str = "", suffix: str = "") -> list[str]:
    return [o["Key"] for o in list_objects(bucket, prefix) if o["Key"].endswith(suffix)]


def list_partitions(bucket: str, name: str = "ingest_date") -> list[str]:
    """Valeurs des partitions Hive « name=valeur/ » présentes à la racine du bucket."""
    resp = get_s3().list_objects_v2(Bucket=bucket, Delimiter="/", Prefix=f"{name}=")
    values = [p["Prefix"].rstrip("/").split("=", 1)[1] for p in resp.get("CommonPrefixes", [])]
    return sorted(values)


def delete_prefix(bucket: str, prefix: str) -> int:
    """Supprime tous les objets d'un préfixe (réécriture idempotente d'une partition)."""
    s3 = get_s3()
    keys = [o["Key"] for o in list_objects(bucket, prefix)]
    for i in range(0, len(keys), 1000):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys[i : i + 1000]]})
    return len(keys)


# ── Objets bruts ─────────────────────────────────────────────────────────────

def put_bytes(bucket: str, key: str, data: bytes, content_type: str = "application/octet-stream",
              metadata: dict | None = None):
    get_s3().put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type,
                        Metadata={k: str(v) for k, v in (metadata or {}).items()})


def get_bytes(bucket: str, key: str) -> bytes:
    return get_s3().get_object(Bucket=bucket, Key=key)["Body"].read()


def put_json(bucket: str, key: str, payload) -> None:
    put_bytes(bucket, key, json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8"),
              "application/json")


def get_json(bucket: str, key: str, default=None):
    try:
        return json.loads(get_bytes(bucket, key))
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return default
        raise


# ── Bronze : JSON Lines ──────────────────────────────────────────────────────

def put_jsonl_lines(bucket: str, key: str, lines: list[str], metadata: dict | None = None):
    """Écrit des lignes déjà sérialisées, SANS les modifier (fidélité Bronze)."""
    body = ("\n".join(line.rstrip("\n") for line in lines) + "\n").encode("utf-8")
    put_bytes(bucket, key, body, "application/x-ndjson", {"rows": len(lines), **(metadata or {})})


def put_jsonl_records(bucket: str, key: str, records: list[dict], metadata: dict | None = None):
    put_jsonl_lines(bucket, key, [json.dumps(r, ensure_ascii=False) for r in records], metadata)


def iter_jsonl_lines(bucket: str, key: str) -> Iterator[str]:
    """Lit un objet JSON Lines en flux, ligne par ligne (sans tout charger en mémoire)."""
    body = get_s3().get_object(Bucket=bucket, Key=key)["Body"]
    for raw in body.iter_lines():
        line = raw.decode("utf-8")
        if line.strip():
            yield line


# ── Silver : Parquet ─────────────────────────────────────────────────────────

def put_parquet(bucket: str, key: str, df, schema=None) -> int:
    """
    Écrit un DataFrame pandas en Parquet, conformément à `schema` (src/silver/schemas.py).
    Les timestamps sont écrits en microsecondes : Spark 3.5 refuse le type
    INT64 TIMESTAMP(NANOS) produit par défaut par pandas.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    if schema is not None:
        df = df[[f.name for f in schema]]
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False, safe=False)
    else:
        table = pa.Table.from_pandas(df, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, coerce_timestamps="us", allow_truncated_timestamps=True,
                   compression="snappy")
    put_bytes(bucket, key, buffer.getvalue(), "application/vnd.apache.parquet", {"rows": len(df)})
    return len(df)


def read_parquet(bucket: str, key: str):
    import pandas as pd

    return pd.read_parquet(io.BytesIO(get_bytes(bucket, key)), engine="pyarrow")


def read_parquet_prefix(bucket: str, prefix: str, max_rows: int | None = None):
    """Concatène les fichiers Parquet d'un préfixe (option : s'arrêter après max_rows lignes)."""
    import pandas as pd

    frames, total = [], 0
    for key in list_keys(bucket, prefix, ".parquet"):
        df = read_parquet(bucket, key)
        frames.append(df)
        total += len(df)
        if max_rows is not None and total >= max_rows:
            break
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    return df.head(max_rows) if max_rows is not None else df


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
