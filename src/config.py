"""Central configuration for the medallion data lake pipeline.

All path conventions, bucket names, and layer prefixes live here so the
bronze, silver, and gold jobs share one source of truth. The mock local run
and a real AWS deployment differ only in the values loaded here. The domain
follows the ZTM Data Engineering course: Airbnb listings and reviews.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# Bucket that holds the entire lake. Overridable so the same code targets a
# real S3 bucket by exporting LAKE_BUCKET before running. The course uses
# ztm-data-engineering-bootcamp; the default mirrors that naming.
LAKE_BUCKET: str = os.environ.get("LAKE_BUCKET", "ztm-data-engineering-bootcamp")

# AWS region used by both moto and a real deployment.
AWS_REGION: str = os.environ.get("AWS_REGION", "us-east-1")

# S3 key prefixes for each medallion layer. These double as the on-disk
# directory names when Spark reads and writes the local lake root.
BRONZE_PREFIX: str = "bronze"
SILVER_PREFIX: str = "silver"
GOLD_PREFIX: str = "gold"

# The two source datasets that make up the Airbnb domain.
LISTINGS_DATASET: str = "listings"
REVIEWS_DATASET: str = "reviews"

# Local filesystem root that mirrors the S3 lake layout. Spark reads and
# writes here because the Hadoop s3a client against an in-process moto
# endpoint is unreliable. The bronze landing and listing are still exercised
# against mock S3 through boto3, proving the S3 object conventions.
LAKE_ROOT: Path = Path(os.environ.get("LAKE_ROOT", "data/lake")).resolve()


@dataclass(frozen=True)
class LayerPaths:
    """Resolved local and S3 locations for a single medallion layer."""

    prefix: str
    local_root: Path = field(default=LAKE_ROOT)

    @property
    def local_path(self) -> Path:
        """Absolute local directory for this layer."""
        return self.local_root / self.prefix

    @property
    def s3_prefix(self) -> str:
        """S3 key prefix (without bucket) for this layer."""
        return f"{self.prefix}/"

    def s3_uri(self, bucket: str = LAKE_BUCKET) -> str:
        """Full s3:// URI for this layer in the given bucket."""
        return f"s3://{bucket}/{self.prefix}/"

    def dataset_local_path(self, dataset: str) -> Path:
        """Local directory for one dataset within this layer."""
        return self.local_path / dataset


BRONZE = LayerPaths(BRONZE_PREFIX)
SILVER = LayerPaths(SILVER_PREFIX)
GOLD = LayerPaths(GOLD_PREFIX)


def bronze_partition_key(dataset: str, ingest_date: str) -> str:
    """Return the bronze S3 key prefix for a dataset and ingest date.

    Args:
        dataset: Source dataset name, listings or reviews.
        ingest_date: Partition date as an ISO string, for example 2024-11-30.

    Returns:
        S3 key prefix such as bronze/reviews/ingest_date=2024-11-30/.
    """
    return f"{BRONZE_PREFIX}/{dataset}/ingest_date={ingest_date}/"
