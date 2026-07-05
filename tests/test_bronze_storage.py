"""Tests for the bronze landing zone against mock S3."""

from __future__ import annotations

from moto import mock_aws

from src import bronze, config, storage


@mock_aws
def test_land_and_count_roundtrip() -> None:
    """Landed reviews are readable back through the S3 API by prefix."""
    client = storage.s3_client()
    storage.ensure_bucket(client)
    reviews = [{"id": i, "listing_id": 1000 + i, "rating": 5} for i in range(25)]
    landed = bronze.land_dataset(client, config.REVIEWS_DATASET, reviews, ingest_date="2024-11-30")
    assert landed == 25
    assert bronze.count_landed(client, config.REVIEWS_DATASET, ingest_date="2024-11-30") == 25


@mock_aws
def test_landing_uses_partitioned_key() -> None:
    """The bronze object key follows the dataset and ingest-date convention."""
    client = storage.s3_client()
    storage.ensure_bucket(client)
    bronze.land_dataset(client, config.LISTINGS_DATASET, [{"id": 1}], ingest_date="2024-11-30")
    keys = list(storage.list_objects(client, "bronze/listings/"))
    assert keys == ["bronze/listings/ingest_date=2024-11-30/listings.json"]
