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


def test_clean_bookings_drops_dirty_records(spark: SparkSession) -> None:
    """Cleaning removes null keys, bad dates, non-positive nights, and bad amounts."""
    raw = spark.createDataFrame(
        [
            (7001, 1000, 501, "2024-05-01", "2024-05-04", "3", "300.0", "completed"),
            (7002, None, 502, "2024-05-01", "2024-05-04", "3", "300.0", "completed"),  # null listing
            (7003, 1002, 503, "not-a-date", "2024-05-04", "3", "300.0", "confirmed"),  # bad checkin
            (7004, 1003, 504, "2024-05-01", "2024-05-04", "0", "300.0", "completed"),  # zero nights
            (7005, 1004, 505, "2024-05-01", "2024-05-04", "3", "-1.0", "completed"),  # bad amount
        ],
        ["booking_id", "listing_id", "guest_id", "checkin_date", "checkout_date", "nights", "amount", "status"],
    )
    cleaned = silver.clean_bookings(raw)
    assert cleaned.count() == 1
    assert cleaned.collect()[0]["booking_id"] == 7001


def test_clean_bookings_deduplicates(spark: SparkSession) -> None:
    """Duplicate booking ids collapse to a single row."""
    raw = spark.createDataFrame(
        [
            (7010, 1000, 501, "2024-06-01", "2024-06-03", "2", "200.0", "completed"),
            (7010, 1000, 501, "2024-06-01", "2024-06-03", "2", "200.0", "completed"),
        ],
        ["booking_id", "listing_id", "guest_id", "checkin_date", "checkout_date", "nights", "amount", "status"],
    )
    assert silver.clean_bookings(raw).count() == 1


def test_clean_transactions_drops_dirty_records(spark: SparkSession) -> None:
    """Cleaning removes null keys, bad timestamps, and blank currency or method."""
    raw = spark.createDataFrame(
        [
            (9001, 7001, "2024-05-01T10:00:00", "300.0", "USD", "card", "succeeded"),
            (9002, None, "2024-05-01T10:00:00", "300.0", "USD", "card", "succeeded"),  # null booking
            (9003, 7003, "not-a-timestamp", "300.0", "USD", "card", "failed"),  # bad ts
            (9004, 7004, "2024-05-01T10:00:00", "300.0", "  ", "card", "succeeded"),  # blank currency
            (9005, 7005, "2024-05-01T10:00:00", "300.0", "USD", "", "succeeded"),  # blank method
        ],
        ["txn_id", "booking_id", "ts", "amount", "currency", "payment_method", "status"],
    )
    cleaned = silver.clean_transactions(raw)
    assert cleaned.count() == 1
    assert cleaned.collect()[0]["txn_id"] == 9001
