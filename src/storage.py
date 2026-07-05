"""S3 storage helpers built on boto3.

These functions work identically against a moto-mocked S3 endpoint and a
real AWS account. They are used for the bronze landing zone, which lands raw
JSON objects and lists them back, demonstrating real S3 key conventions and
partitioned object layout.
"""

from __future__ import annotations

from typing import Iterator

import boto3
from botocore.client import BaseClient

from . import config


def s3_client() -> BaseClient:
    """Create an S3 client for the configured region.

    Under moto the AWS calls are intercepted in-process. Against real AWS the
    standard credential chain (environment, profile, or instance role) applies
    with no code change.
    """
    return boto3.client("s3", region_name=config.AWS_REGION)


def ensure_bucket(client: BaseClient, bucket: str = config.LAKE_BUCKET) -> None:
    """Create the lake bucket if it does not already exist.

    us-east-1 rejects an explicit LocationConstraint, so it is omitted for
    that region and supplied for every other region.
    """
    existing = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
    if bucket in existing:
        return
    if config.AWS_REGION == "us-east-1":
        client.create_bucket(Bucket=bucket)
    else:
        client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": config.AWS_REGION},
        )


def put_json_object(client: BaseClient, key: str, body: str, bucket: str = config.LAKE_BUCKET) -> None:
    """Upload a single JSON payload to an S3 key.

    Args:
        client: An S3 client.
        key: Full object key, including layer prefix and partition path.
        body: The JSON string to store.
        bucket: Target bucket name.
    """
    client.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"), ContentType="application/json")


def list_objects(client: BaseClient, prefix: str, bucket: str = config.LAKE_BUCKET) -> Iterator[str]:
    """Yield every object key under a prefix, following pagination.

    Args:
        client: An S3 client.
        prefix: S3 key prefix to list.
        bucket: Bucket to list from.

    Yields:
        Object keys in lexicographic order.
    """
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]


def get_json_object(client: BaseClient, key: str, bucket: str = config.LAKE_BUCKET) -> str:
    """Download a single object and return its body as a UTF-8 string."""
    response = client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf-8")
