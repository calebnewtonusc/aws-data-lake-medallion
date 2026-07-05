"""Silver layer: clean, validate, dedupe, and type-cast bronze data.

The silver job reads raw JSON for both the listings and reviews datasets with
explicit schemas (schema-on-read), drops records that fail validation,
deduplicates on the natural key, casts columns to their target types, and
writes parquet to the silver layer. The reviews table is partitioned by
review month; the listings dimension is written unpartitioned. Writes are
idempotent using dynamic partition overwrite, so re-running replaces only the
partitions present in the new data.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from . import config

# Explicit schemas applied on read. Numeric and date fields land as strings
# first so a malformed value does not crash the parse; casting and validation
# then happen with full control.
LISTINGS_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("host_id", LongType(), True),
        StructField("neighbourhood", StringType(), True),
        StructField("room_type", StringType(), True),
        StructField("price", StringType(), True),
        StructField("minimum_nights", StringType(), True),
    ]
)

REVIEWS_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("listing_id", LongType(), True),
        StructField("date", StringType(), True),
        StructField("reviewer_id", LongType(), True),
        StructField("reviewer_name", StringType(), True),
        StructField("rating", StringType(), True),
        StructField("comments", StringType(), True),
    ]
)


def _read_bronze(spark: SparkSession, dataset: str, schema: StructType) -> DataFrame:
    """Read one raw bronze dataset into a DataFrame using an explicit schema."""
    return spark.read.schema(schema).json(str(config.BRONZE.dataset_local_path(dataset)))


def clean_listings(raw: DataFrame) -> DataFrame:
    """Cast, validate, and deduplicate raw listings.

    Rules: cast price and minimum_nights to numerics, drop rows with a null id
    or name or failed numeric casts, drop non-positive prices, then keep one
    row per listing id.

    Args:
        raw: Raw listings DataFrame.

    Returns:
        A cleaned, typed, deduplicated listings dimension.
    """
    typed = raw.withColumn("price", F.col("price").cast(DoubleType())).withColumn(
        "minimum_nights", F.col("minimum_nights").cast(IntegerType())
    )
    valid = typed.where(
        F.col("id").isNotNull()
        & (F.trim(F.col("name")) != "")
        & F.col("price").isNotNull()
        & (F.col("price") > 0)
        & F.col("minimum_nights").isNotNull()
    )
    dedupe = Window.partitionBy("id").orderBy(F.col("host_id").asc())
    return valid.withColumn("_rank", F.row_number().over(dedupe)).where(F.col("_rank") == 1).drop("_rank")


def clean_reviews(raw: DataFrame) -> DataFrame:
    """Cast, validate, and deduplicate raw reviews.

    Rules: parse date to a real date and rating to an integer, drop rows with a
    null review id, listing id, reviewer id, or failed date parse, drop blank
    comments, drop ratings outside the 1 to 5 range, then keep one row per
    review id. A review_month partition column is derived from the parsed date.

    Args:
        raw: Raw reviews DataFrame.

    Returns:
        A cleaned, typed, deduplicated reviews fact table.
    """
    typed = (
        raw.withColumn("review_date", F.to_date("date"))
        .drop("date")
        .withColumn("rating", F.col("rating").cast(IntegerType()))
    )
    valid = typed.where(
        F.col("id").isNotNull()
        & F.col("listing_id").isNotNull()
        & F.col("reviewer_id").isNotNull()
        & F.col("review_date").isNotNull()
        & (F.trim(F.col("comments")) != "")
        & F.col("rating").isNotNull()
        & (F.col("rating") >= 1)
        & (F.col("rating") <= 5)
    )
    with_month = valid.withColumn("review_month", F.date_format("review_date", "yyyy-MM"))
    dedupe = Window.partitionBy("id").orderBy(F.col("review_date").asc())
    return (
        with_month.withColumn("_rank", F.row_number().over(dedupe)).where(F.col("_rank") == 1).drop("_rank")
    )


def write_listings(df: DataFrame) -> None:
    """Write the cleaned listings dimension to silver as parquet."""
    df.write.mode("overwrite").parquet(str(config.SILVER.dataset_local_path(config.LISTINGS_DATASET)))


def write_reviews(df: DataFrame) -> None:
    """Write the cleaned reviews fact table to silver, partitioned by month."""
    (
        df.write.mode("overwrite")
        .partitionBy("review_month")
        .parquet(str(config.SILVER.dataset_local_path(config.REVIEWS_DATASET)))
    )


def run(spark: SparkSession) -> dict[str, int]:
    """Execute the full silver job and return cleaned row counts per dataset."""
    listings = clean_listings(_read_bronze(spark, config.LISTINGS_DATASET, LISTINGS_SCHEMA))
    reviews = clean_reviews(_read_bronze(spark, config.REVIEWS_DATASET, REVIEWS_SCHEMA))
    listings.cache()
    reviews.cache()
    counts = {"listings": listings.count(), "reviews": reviews.count()}
    write_listings(listings)
    write_reviews(reviews)
    listings.unpersist()
    reviews.unpersist()
    return counts
