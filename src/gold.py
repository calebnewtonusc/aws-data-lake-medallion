"""Gold layer: business-ready Airbnb aggregates from the silver layer.

The gold job reads the cleaned silver listings dimension and reviews fact
table, joins them on listing id, and produces three curated parquet tables:
reviews-per-listing (the course's flagship aggregate), average rating per
listing, and reviews per neighbourhood. These are the tables an analyst or BI
tool queries, and the ones the Athena DDL in sql/athena_ddl.sql points at.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import config


def read_silver_listings(spark: SparkSession) -> DataFrame:
    """Read the cleaned silver listings dimension."""
    return spark.read.parquet(str(config.SILVER.dataset_local_path(config.LISTINGS_DATASET)))


def read_silver_reviews(spark: SparkSession) -> DataFrame:
    """Read the cleaned silver reviews fact table."""
    return spark.read.parquet(str(config.SILVER.dataset_local_path(config.REVIEWS_DATASET)))


def reviews_per_listing(listings: DataFrame, reviews: DataFrame) -> DataFrame:
    """Count reviews per listing, ranked most reviewed first.

    This mirrors the course's reviews-per-listing Spark job: an inner join of
    listings and reviews on the listing id, grouped by listing, counting
    reviews and ordering descending.

    Args:
        listings: The silver listings dimension.
        reviews: The silver reviews fact table.

    Returns:
        A DataFrame of listing id, name, neighbourhood, and review count.
    """
    joined = listings.join(reviews, listings.id == reviews.listing_id, how="inner")
    return (
        joined.groupBy(listings.id, listings.name, listings.neighbourhood)
        .agg(F.count(reviews.id).alias("num_reviews"))
        .orderBy(F.col("num_reviews").desc())
    )


def avg_rating_per_listing(listings: DataFrame, reviews: DataFrame) -> DataFrame:
    """Average review rating per listing, ranked highest rated first.

    Args:
        listings: The silver listings dimension.
        reviews: The silver reviews fact table.

    Returns:
        A DataFrame of listing id, name, average rating, and review count.
    """
    joined = listings.join(reviews, listings.id == reviews.listing_id, how="inner")
    return (
        joined.groupBy(listings.id, listings.name)
        .agg(
            F.round(F.avg(reviews.rating), 2).alias("avg_rating"),
            F.count(reviews.id).alias("num_reviews"),
        )
        .orderBy(F.col("avg_rating").desc(), F.col("num_reviews").desc())
    )


def reviews_per_neighbourhood(listings: DataFrame, reviews: DataFrame) -> DataFrame:
    """Total reviews and average rating aggregated by neighbourhood.

    Args:
        listings: The silver listings dimension.
        reviews: The silver reviews fact table.

    Returns:
        A DataFrame of neighbourhood, review count, and average rating.
    """
    joined = listings.join(reviews, listings.id == reviews.listing_id, how="inner")
    return (
        joined.groupBy(listings.neighbourhood)
        .agg(
            F.count(reviews.id).alias("num_reviews"),
            F.round(F.avg(reviews.rating), 2).alias("avg_rating"),
            F.countDistinct(listings.id).alias("num_listings"),
        )
        .orderBy(F.col("num_reviews").desc())
    )


def _write(df: DataFrame, name: str) -> int:
    """Write one gold table as parquet under gold/<name>/ and return its rows."""
    target = config.GOLD.local_path / name
    df.write.mode("overwrite").parquet(str(target))
    return df.count()


def run(spark: SparkSession) -> dict[str, int]:
    """Execute the gold job and return row counts keyed by table name."""
    listings = read_silver_listings(spark)
    reviews = read_silver_reviews(spark)
    listings.cache()
    reviews.cache()
    counts = {
        "reviews_per_listing": _write(reviews_per_listing(listings, reviews), "reviews_per_listing"),
        "avg_rating_per_listing": _write(avg_rating_per_listing(listings, reviews), "avg_rating_per_listing"),
        "reviews_per_neighbourhood": _write(reviews_per_neighbourhood(listings, reviews), "reviews_per_neighbourhood"),
    }
    listings.unpersist()
    reviews.unpersist()
    return counts
