"""EMR Serverless entrypoint for the Airbnb medallion pipeline on real S3.

This is a single, self-contained PySpark job that runs the same medallion
logic as the local pipeline, but reads and writes s3:// paths directly. It is
designed to be uploaded to S3 and submitted as an EMR Serverless Spark job
run. It has no dependency on the src package or on moto, so nothing needs to
be installed on the EMR workers.

Layers:
  bronze  reads raw newline-delimited JSON that 01_create_bucket.sh uploaded
          to s3://<bucket>/bronze/<dataset>/ingest_date=<date>/<dataset>.json
  silver  cleans, validates, type-casts, and deduplicates, then writes parquet
          to s3://<bucket>/silver/<dataset>/
  gold    joins the silver tables and writes the three business aggregates to
          s3://<bucket>/gold/<name>/

The cleaning and aggregation logic is intentionally identical to
src/silver.py and src/gold.py so the local moto run and the EMR run produce
the same tables. If you change one, change the other.

Usage (arguments are passed as entryPointArguments in the job run):
    medallion_emr.py \
        --bronze s3://BUCKET/bronze \
        --silver s3://BUCKET/silver \
        --gold   s3://BUCKET/gold
"""

from __future__ import annotations

import argparse
import sys

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

LISTINGS_DATASET = "listings"
REVIEWS_DATASET = "reviews"

# Explicit schemas applied on read. Numeric and date fields land as strings
# first so a malformed value does not crash the parse; casting and validation
# then happen with full control. Mirrors src/silver.py.
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


def _log(message: str) -> None:
    """Print a progress line that shows up in the EMR driver stdout log."""
    print(f"[medallion_emr] {message}", flush=True)


def clean_listings(raw: DataFrame) -> DataFrame:
    """Cast, validate, and deduplicate raw listings. Mirrors src/silver.py."""
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
    """Cast, validate, and deduplicate raw reviews. Mirrors src/silver.py."""
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
    return with_month.withColumn("_rank", F.row_number().over(dedupe)).where(F.col("_rank") == 1).drop("_rank")


def reviews_per_listing(listings: DataFrame, reviews: DataFrame) -> DataFrame:
    """Count reviews per listing, ranked most reviewed first. Mirrors src/gold.py."""
    joined = listings.join(reviews, listings.id == reviews.listing_id, how="inner")
    return (
        joined.groupBy(listings.id, listings.name, listings.neighbourhood)
        .agg(F.count(reviews.id).alias("num_reviews"))
        .orderBy(F.col("num_reviews").desc())
    )


def avg_rating_per_listing(listings: DataFrame, reviews: DataFrame) -> DataFrame:
    """Average review rating per listing. Mirrors src/gold.py."""
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
    """Total reviews and average rating by neighbourhood. Mirrors src/gold.py."""
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


def _read_bronze(spark: SparkSession, bronze_base: str, dataset: str, schema: StructType) -> DataFrame:
    """Read every JSON object under bronze/<dataset>/ across ingest_date partitions."""
    path = f"{bronze_base.rstrip('/')}/{dataset}/"
    _log(f"reading bronze {dataset} from {path}")
    return spark.read.schema(schema).option("recursiveFileLookup", "true").json(path)


def run(spark: SparkSession, bronze_base: str, silver_base: str, gold_base: str) -> None:
    """Read bronze from S3, build silver and gold, and write parquet back to S3."""
    silver_base = silver_base.rstrip("/")
    gold_base = gold_base.rstrip("/")

    _log("=== SILVER: clean, validate, deduplicate ===")
    listings = clean_listings(_read_bronze(spark, bronze_base, LISTINGS_DATASET, LISTINGS_SCHEMA))
    reviews = clean_reviews(_read_bronze(spark, bronze_base, REVIEWS_DATASET, REVIEWS_SCHEMA))
    listings.cache()
    reviews.cache()
    n_listings = listings.count()
    n_reviews = reviews.count()
    _log(f"silver listings rows: {n_listings}")
    _log(f"silver reviews rows: {n_reviews}")

    listings_out = f"{silver_base}/{LISTINGS_DATASET}"
    reviews_out = f"{silver_base}/{REVIEWS_DATASET}"
    _log(f"writing silver listings to {listings_out}")
    listings.write.mode("overwrite").parquet(listings_out)
    _log(f"writing silver reviews to {reviews_out} (partitioned by review_month)")
    reviews.write.mode("overwrite").partitionBy("review_month").parquet(reviews_out)

    _log("=== DATA QUALITY: gate the silver reviews table ===")
    row_count = reviews.count()
    distinct_ids = reviews.select("id").distinct().count()
    null_keys = reviews.where(F.col("id").isNull() | F.col("listing_id").isNull()).count()
    bad_ratings = reviews.where((F.col("rating") < 1) | (F.col("rating") > 5)).count()
    duplicates = row_count - distinct_ids
    if row_count == 0:
        raise RuntimeError("Silver reviews table is empty; ingestion or cleaning failed.")
    if duplicates > 0:
        raise RuntimeError(f"Found {duplicates} duplicate review ids after dedupe.")
    if null_keys > 0:
        raise RuntimeError(f"Found {null_keys} reviews with a null review or listing key.")
    if bad_ratings > 0:
        raise RuntimeError(f"Found {bad_ratings} reviews with a rating outside 1 to 5.")
    _log(f"quality passed: {row_count} reviews, {distinct_ids} unique ids, 0 duplicates, 0 null keys, 0 bad ratings")

    _log("=== GOLD: build business aggregates ===")
    gold_tables = {
        "reviews_per_listing": reviews_per_listing(listings, reviews),
        "avg_rating_per_listing": avg_rating_per_listing(listings, reviews),
        "reviews_per_neighbourhood": reviews_per_neighbourhood(listings, reviews),
    }
    for name, df in gold_tables.items():
        target = f"{gold_base}/{name}"
        rows = df.count()
        _log(f"writing gold {name}: {rows} rows to {target}")
        df.write.mode("overwrite").parquet(target)

    listings.unpersist()
    reviews.unpersist()
    _log("=== PIPELINE COMPLETE: silver and gold written to S3 ===")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Airbnb medallion pipeline on EMR Serverless.")
    parser.add_argument("--bronze", required=True, help="s3:// base for bronze, e.g. s3://BUCKET/bronze")
    parser.add_argument("--silver", required=True, help="s3:// base for silver, e.g. s3://BUCKET/silver")
    parser.add_argument("--gold", required=True, help="s3:// base for gold, e.g. s3://BUCKET/gold")
    return parser.parse_args(argv)


def main(argv: list[str]) -> None:
    """Build a Spark session and run the medallion job against S3 paths."""
    args = _parse_args(argv)
    spark = (
        SparkSession.builder.appName("airbnb-medallion-emr")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        run(spark, args.bronze, args.silver, args.gold)
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
