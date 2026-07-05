"""Data-quality gate for the silver reviews fact table.

The pipeline runs these checks after the silver write and before the gold
aggregations. Any failed assertion raises DataQualityError, halting the
pipeline loudly rather than propagating bad data into business tables.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityError(RuntimeError):
    """Raised when a silver-layer data-quality check fails."""


@dataclass(frozen=True)
class QualityReport:
    """Summary of the checks run against the silver reviews table."""

    row_count: int
    distinct_review_ids: int
    null_key_count: int
    out_of_range_rating_count: int

    @property
    def duplicate_review_ids(self) -> int:
        """Number of review ids that appear more than once."""
        return self.row_count - self.distinct_review_ids


def run_checks(reviews: DataFrame) -> QualityReport:
    """Validate the silver reviews DataFrame and return a report, or fail.

    Checks enforced:
    the table is non-empty, review id is unique after dedupe, no null review or
    listing keys survived, and every rating is within the 1 to 5 range.

    Args:
        reviews: The cleaned silver reviews DataFrame.

    Returns:
        A QualityReport describing the validated data.

    Raises:
        DataQualityError: If any check fails.
    """
    row_count = reviews.count()
    distinct_review_ids = reviews.select("id").distinct().count()
    null_key_count = reviews.where(F.col("id").isNull() | F.col("listing_id").isNull()).count()
    out_of_range_rating_count = reviews.where((F.col("rating") < 1) | (F.col("rating") > 5)).count()

    report = QualityReport(
        row_count=row_count,
        distinct_review_ids=distinct_review_ids,
        null_key_count=null_key_count,
        out_of_range_rating_count=out_of_range_rating_count,
    )

    if row_count == 0:
        raise DataQualityError("Silver reviews table is empty; upstream ingestion or cleaning failed.")
    if report.duplicate_review_ids > 0:
        raise DataQualityError(f"Found {report.duplicate_review_ids} duplicate review ids after dedupe.")
    if null_key_count > 0:
        raise DataQualityError(f"Found {null_key_count} reviews with a null review or listing key.")
    if out_of_range_rating_count > 0:
        raise DataQualityError(f"Found {out_of_range_rating_count} reviews with a rating outside 1 to 5.")

    return report
