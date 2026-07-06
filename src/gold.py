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


def read_silver_bookings(spark: SparkSession) -> DataFrame:
    """Read the cleaned silver bookings fact table."""
    return spark.read.parquet(str(config.SILVER.dataset_local_path(config.BOOKINGS_DATASET)))


def read_silver_transactions(spark: SparkSession) -> DataFrame:
    """Read the cleaned silver transactions fact table."""
    return spark.read.parquet(str(config.SILVER.dataset_local_path(config.TRANSACTIONS_DATASET)))


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


def revenue_by_listing_month(listings: DataFrame, bookings: DataFrame) -> DataFrame:
    """Completed-booking revenue per listing per check-in month.

    Only bookings in a completed state count toward realized revenue. The
    result is partitioned by check-in month downstream so time-bounded revenue
    queries prune to a single month.

    Args:
        listings: The silver listings dimension.
        bookings: The silver bookings fact table.

    Returns:
        A DataFrame of listing id, name, neighbourhood, checkin_month, booking
        count, total nights, and total revenue.
    """
    completed = bookings.where(F.col("status") == "completed")
    joined = listings.join(completed, listings.id == completed.listing_id, how="inner")
    return (
        joined.groupBy(listings.id, listings.name, listings.neighbourhood, completed.checkin_month)
        .agg(
            F.count(completed.booking_id).alias("num_bookings"),
            F.sum(completed.nights).alias("total_nights"),
            F.round(F.sum(completed.amount), 2).alias("total_revenue"),
        )
        .orderBy(F.col("total_revenue").desc())
    )


def booking_conversion(listings: DataFrame, bookings: DataFrame) -> DataFrame:
    """Booking conversion and cancellation rates per listing.

    Conversion is completed-plus-confirmed bookings over total bookings; the
    cancellation rate is cancelled-plus-no-show bookings over total. These are
    the funnel-health numbers a marketplace analyst watches per listing.

    Args:
        listings: The silver listings dimension.
        bookings: The silver bookings fact table.

    Returns:
        A DataFrame of listing id, name, total bookings, completed, confirmed,
        cancelled, conversion_rate, and cancellation_rate.
    """
    joined = listings.join(bookings, listings.id == bookings.listing_id, how="inner")
    grouped = joined.groupBy(listings.id, listings.name).agg(
        F.count(bookings.booking_id).alias("total_bookings"),
        F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("completed"),
        F.sum(F.when(F.col("status") == "confirmed", 1).otherwise(0)).alias("confirmed"),
        F.sum(F.when(F.col("status").isin("cancelled", "no_show"), 1).otherwise(0)).alias("cancelled"),
    )
    return grouped.withColumn(
        "conversion_rate",
        F.round((F.col("completed") + F.col("confirmed")) / F.col("total_bookings"), 4),
    ).withColumn(
        "cancellation_rate", F.round(F.col("cancelled") / F.col("total_bookings"), 4)
    ).orderBy(F.col("total_bookings").desc())


def transaction_success_rates(transactions: DataFrame) -> DataFrame:
    """Payment success, failure, and refund rates per payment method.

    Args:
        transactions: The silver transactions fact table.

    Returns:
        A DataFrame of payment_method, total transactions, succeeded, failed,
        refunded, gross settled amount, and success_rate.
    """
    grouped = transactions.groupBy("payment_method").agg(
        F.count("txn_id").alias("total_txns"),
        F.sum(F.when(F.col("status") == "succeeded", 1).otherwise(0)).alias("succeeded"),
        F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)).alias("failed"),
        F.sum(F.when(F.col("status") == "refunded", 1).otherwise(0)).alias("refunded"),
        F.round(F.sum(F.when(F.col("status") == "succeeded", F.col("amount")).otherwise(0.0)), 2).alias(
            "settled_amount"
        ),
    )
    return grouped.withColumn(
        "success_rate", F.round(F.col("succeeded") / F.col("total_txns"), 4)
    ).orderBy(F.col("total_txns").desc())


def _write(df: DataFrame, name: str, partition_by: str | None = None) -> int:
    """Write one gold table as parquet under gold/<name>/ and return its rows.

    Args:
        df: The DataFrame to write.
        name: Gold table directory name.
        partition_by: Optional column to partition the output by.

    Returns:
        The row count written.
    """
    target = config.GOLD.local_path / name
    writer = df.write.mode("overwrite")
    if partition_by is not None:
        writer = writer.partitionBy(partition_by)
    writer.parquet(str(target))
    return df.count()


def run(spark: SparkSession) -> dict[str, int]:
    """Execute the gold job and return row counts keyed by table name.

    The listings-and-reviews aggregates always build. The revenue, conversion,
    and payment-success tables build only when their silver inputs are present,
    so partial runs still succeed.
    """
    listings = read_silver_listings(spark)
    reviews = read_silver_reviews(spark)
    listings.cache()
    reviews.cache()
    counts = {
        "reviews_per_listing": _write(reviews_per_listing(listings, reviews), "reviews_per_listing"),
        "avg_rating_per_listing": _write(avg_rating_per_listing(listings, reviews), "avg_rating_per_listing"),
        "reviews_per_neighbourhood": _write(reviews_per_neighbourhood(listings, reviews), "reviews_per_neighbourhood"),
    }

    bookings_path = config.SILVER.dataset_local_path(config.BOOKINGS_DATASET)
    if bookings_path.exists():
        bookings = read_silver_bookings(spark)
        bookings.cache()
        counts["revenue_by_listing_month"] = _write(
            revenue_by_listing_month(listings, bookings), "revenue_by_listing_month", partition_by="checkin_month"
        )
        counts["booking_conversion"] = _write(booking_conversion(listings, bookings), "booking_conversion")
        bookings.unpersist()

    txns_path = config.SILVER.dataset_local_path(config.TRANSACTIONS_DATASET)
    if txns_path.exists():
        transactions = read_silver_transactions(spark)
        counts["transaction_success_rates"] = _write(
            transaction_success_rates(transactions), "transaction_success_rates"
        )

    listings.unpersist()
    reviews.unpersist()
    return counts
