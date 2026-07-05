"""Tests for silver cleaning and the data-quality gate using local Spark."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from src import quality, silver


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Provide a lightweight local SparkSession for the test module."""
    session = (
        SparkSession.builder.appName("medallion-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_clean_reviews_drops_dirty_records(spark: SparkSession) -> None:
    """Cleaning removes null keys, blank comments, bad dates, and bad ratings."""
    raw = spark.createDataFrame(
        [
            (1, 1000, "2024-05-01", 55, "Ann", "3", "Great place"),
            (2, None, "2024-05-02", 66, "Bo", "4", "Nice"),  # null listing
            (3, 1002, "not-a-date", 77, "Cy", "5", "Cozy"),  # bad date
            (4, 1003, "2024-05-04", 88, "Di", "2", "   "),  # blank comment
            (5, 1004, "2024-05-05", 99, "Ed", "0", "Loud"),  # bad rating
        ],
        ["id", "listing_id", "date", "reviewer_id", "reviewer_name", "rating", "comments"],
    )
    cleaned = silver.clean_reviews(raw)
    assert cleaned.count() == 1
    assert cleaned.collect()[0]["id"] == 1


def test_clean_reviews_deduplicates(spark: SparkSession) -> None:
    """Duplicate review ids collapse to a single row."""
    raw = spark.createDataFrame(
        [
            (10, 1000, "2024-06-01", 55, "Ann", "4", "Good"),
            (10, 1000, "2024-06-01", 55, "Ann", "4", "Good"),
        ],
        ["id", "listing_id", "date", "reviewer_id", "reviewer_name", "rating", "comments"],
    )
    assert silver.clean_reviews(raw).count() == 1


def test_quality_gate_passes_clean_data(spark: SparkSession) -> None:
    """A clean reviews table passes every quality check."""
    reviews = spark.createDataFrame(
        [(1, 1000, 5), (2, 1001, 4)],
        ["id", "listing_id", "rating"],
    )
    report = quality.run_checks(reviews)
    assert report.row_count == 2
    assert report.duplicate_review_ids == 0


def test_quality_gate_fails_on_duplicates(spark: SparkSession) -> None:
    """Duplicate review ids trip the data-quality gate."""
    reviews = spark.createDataFrame(
        [(1, 1000, 5), (1, 1000, 5)],
        ["id", "listing_id", "rating"],
    )
    with pytest.raises(quality.DataQualityError):
        quality.run_checks(reviews)
