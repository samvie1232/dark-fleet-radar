"""Crea el bucket del warehouse en SeaweedFS (idempotente, con reintentos)."""

from __future__ import annotations

import time

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from . import settings


def ensure_bucket(retries: int = 30, wait_s: float = 2.0) -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=settings.S3_ENDPOINT,
        aws_access_key_id=settings.S3_ACCESS_KEY,
        aws_secret_access_key=settings.S3_SECRET_KEY,
        region_name=settings.S3_REGION,
        config=Config(s3={"addressing_style": "path"}),
    )
    for attempt in range(1, retries + 1):
        try:
            existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
            if settings.WAREHOUSE_BUCKET not in existing:
                s3.create_bucket(Bucket=settings.WAREHOUSE_BUCKET)
                print(f"Bucket '{settings.WAREHOUSE_BUCKET}' creado")
            else:
                print(f"Bucket '{settings.WAREHOUSE_BUCKET}' ya existe")
            return
        except (BotoCoreError, ClientError) as exc:
            print(f"S3 aún no responde ({attempt}/{retries}): {exc}")
            time.sleep(wait_s)
    raise SystemExit("No se pudo crear el bucket del warehouse")
