"""Conexión al S3 local (SeaweedFS) y al catálogo REST de Iceberg."""

from __future__ import annotations

import os

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://s3:8333")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "radar")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "radar-local-secret")
S3_REGION = os.environ.get("S3_REGION", "us-east-1")
WAREHOUSE_BUCKET = os.environ.get("WAREHOUSE_BUCKET", "warehouse")
CATALOG_URI = os.environ.get("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181")
NAMESPACE = "ais"


def load_catalog():
    from pyiceberg.catalog import load_catalog as _load

    return _load(
        "lake",
        type="rest",
        uri=CATALOG_URI,
        **{
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": S3_ACCESS_KEY,
            "s3.secret-access-key": S3_SECRET_KEY,
            "s3.region": S3_REGION,
            "s3.path-style-access": "true",
        },
    )
