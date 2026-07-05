"""Bronze layer: land raw Airbnb listings and reviews as-is.

The bronze job takes generated listings and reviews and lands them untouched
into S3 under bronze/<dataset>/ingest_date=<date>/, one JSON object per
landing batch. It uses boto3 so the exact S3 key conventions and partitioning
are exercised against the moto mock, then mirrors the same partition to the
local lake root that Spark reads in the silver stage. No cleaning or type
casting happens here: bronze is the immutable, faithful copy of the source.
"""

from __future__ import annotations

import json
from datetime import date
from typing import Any

from botocore.client import BaseClient

from . import config, storage


def land_dataset(
    client: BaseClient,
    dataset: str,
    records: list[dict[str, Any]],
    ingest_date: str | None = None,
    bucket: str = config.LAKE_BUCKET,
) -> int:
    """Land one raw dataset into bronze, partitioned by ingest date.

    The records are written both to mock S3 (as newline-delimited JSON) and to
    the mirrored local lake root, keeping the object key and the local path in
    lockstep. Writing is idempotent per partition: re-running for the same
    dataset and ingest date overwrites that object rather than appending.

    Args:
        client: An S3 client, real or moto-mocked.
        dataset: Source dataset name, listings or reviews.
        records: Raw record dictionaries.
        ingest_date: ISO ingest date. Defaults to today (UTC).
        bucket: Target lake bucket.

    Returns:
        The number of raw records landed.
    """
    if ingest_date is None:
        ingest_date = date.today().isoformat()

    payload = "\n".join(json.dumps(r) for r in records)
    key = f"{config.bronze_partition_key(dataset, ingest_date)}{dataset}.json"
    storage.put_json_object(client, key, payload, bucket=bucket)

    local_partition = config.BRONZE.dataset_local_path(dataset) / f"ingest_date={ingest_date}"
    local_partition.mkdir(parents=True, exist_ok=True)
    (local_partition / f"{dataset}.json").write_text(payload + "\n", encoding="utf-8")

    return len(records)


def count_landed(
    client: BaseClient,
    dataset: str,
    ingest_date: str | None = None,
    bucket: str = config.LAKE_BUCKET,
) -> int:
    """Count raw records landed in mock S3 for a dataset and ingest date.

    This reads back through the S3 API to prove the objects are queryable by
    prefix, the same way a downstream consumer would discover them.

    Args:
        client: An S3 client.
        dataset: Source dataset name, listings or reviews.
        ingest_date: ISO ingest date. Defaults to today (UTC).
        bucket: Lake bucket to read from.

    Returns:
        Total raw records across all objects in the partition.
    """
    if ingest_date is None:
        ingest_date = date.today().isoformat()
    prefix = config.bronze_partition_key(dataset, ingest_date)
    total = 0
    for key in storage.list_objects(client, prefix, bucket=bucket):
        body = storage.get_json_object(client, key, bucket=bucket)
        total += sum(1 for line in body.splitlines() if line.strip())
    return total
