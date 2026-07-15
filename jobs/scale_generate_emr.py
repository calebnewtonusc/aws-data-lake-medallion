"""EMR Serverless scale-up generator: write 250+ GB of partitioned Parquet to S3.

The local pipeline and benchmark run at a validated smaller scale because a
laptop does not have 250 GB of free disk. This job is the AWS-scale path: it
generates the same four source domains (listings, reviews, bookings,
transactions) at an arbitrary target size and writes them as partitioned,
compacted Parquet directly into S3, where storage is effectively unbounded.

It is self-contained (no dependency on the src package or moto) so it can be
uploaded to S3 and submitted as an EMR Serverless Spark job run, exactly like
jobs/medallion_emr.py. Row counts are computed from a target-GB argument, and
generation uses spark.range plus deterministic hashing so it scales to hundreds
of millions of rows without ever collecting to the driver.

The physical layout it writes is the optimized layout the benchmark measured:
each fact partitioned by month, compacted to right-sized files, sorted within
partition so Parquet row-group statistics enable predicate pushdown. That means
the 250 GB lake on AWS has the same query-optimization properties proven locally,
just at production scale.

Usage (arguments passed as entryPointArguments in the job run):
    scale_generate_emr.py \
        --out s3://BUCKET/scale \
        --target-gb 250 \
        --domain all

--target-gb is the approximate total on-disk size across the four domains. The
transactions fact carries roughly 80% of the volume; bookings and reviews split
most of the rest; listings is a small fixed dimension. Pass --domain to generate
only one domain (transactions | bookings | reviews | listings | all).

Sizing note: at Snappy-compressed Parquet the transactions fact averages about
40 bytes/row on disk here, so 250 GB is on the order of 5 billion transaction
rows. The job prints the row counts it derives from --target-gb before writing
so you can confirm the scale before it runs.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

START_DATE = date(2023, 1, 1)
NUM_DAYS = 365 * 2
NUM_MONTHS = NUM_DAYS // 30 + 2

TXN_STATUSES = ["succeeded", "failed", "refunded"]
BOOKING_STATUSES = ["completed", "confirmed", "cancelled", "no_show"]
CURRENCIES = ["USD", "EUR", "GBP", "AUD"]
METHODS = ["card", "paypal", "apple_pay", "google_pay", "bank_transfer"]
ROOM_TYPES = ["Entire home/apt", "Private room", "Shared room", "Hotel room"]
NEIGHBOURHOODS = [
    "Downtown", "Mission District", "Capitol Hill", "Williamsburg",
    "Shoreditch", "Le Marais", "Kreuzberg", "Fitzroy",
]

# Approximate on-disk bytes per row for each domain at Snappy Parquet, measured
# from the local benchmark's optimized layout. Used to convert --target-gb into
# row counts. Transactions dominate the volume.
BYTES_PER_ROW = {"transactions": 42.0, "bookings": 40.0, "reviews": 30.0}
# Share of total target size allocated to each fact domain.
DOMAIN_SHARE = {"transactions": 0.80, "bookings": 0.12, "reviews": 0.08}
NUM_LISTINGS = 2_000_000  # a larger dimension at AWS scale


def _log(message: str) -> None:
    print(f"[scale_generate_emr] {message}", flush=True)


def _files_for(rows: int) -> int:
    """Files per partition sized so each is roughly 128-256 MB (compaction)."""
    per_partition = max(rows // NUM_MONTHS, 1)
    # target ~4M rows/file -> right-sized Parquet files, avoiding the small-file tax
    return max(1, min(64, per_partition // 4_000_000))


def gen_transactions(spark: SparkSession, rows: int) -> DataFrame:
    base = spark.range(0, rows).withColumnRenamed("id", "txn_id")
    day_offset = ((F.col("txn_id") * F.lit(2654435761)) % F.lit(NUM_DAYS)).cast("int")
    return (
        base.withColumn("booking_id", (F.col("txn_id") % F.lit(rows // 2 + 1)))
        .withColumn("listing_id", (F.col("txn_id") % F.lit(NUM_LISTINGS)))
        .withColumn("day_offset", day_offset)
        .withColumn("txn_date", F.date_add(F.lit(START_DATE.isoformat()).cast("date"), F.col("day_offset")))
        .drop("day_offset")
        .withColumn("txn_month", F.date_format(F.col("txn_date"), "yyyy-MM"))
        .withColumn("amount", F.round((F.rand(seed=13) * F.lit(950.0)) + F.lit(50.0), 2))
        .withColumn("status", F.element_at(F.array(*[F.lit(s) for s in TXN_STATUSES]), (F.col("txn_id") % F.lit(len(TXN_STATUSES)) + 1).cast("int")))
        .withColumn("currency", F.element_at(F.array(*[F.lit(c) for c in CURRENCIES]), (F.col("txn_id") % F.lit(len(CURRENCIES)) + 1).cast("int")))
        .withColumn("payment_method", F.element_at(F.array(*[F.lit(m) for m in METHODS]), (F.col("txn_id") % F.lit(len(METHODS)) + 1).cast("int")))
        .select("txn_id", "booking_id", "listing_id", "txn_date", "txn_month", "amount", "status", "currency", "payment_method")
    )


def gen_bookings(spark: SparkSession, rows: int) -> DataFrame:
    base = spark.range(0, rows).withColumnRenamed("id", "booking_id")
    day_offset = ((F.col("booking_id") * F.lit(40503)) % F.lit(NUM_DAYS)).cast("int")
    return (
        base.withColumn("listing_id", (F.col("booking_id") % F.lit(NUM_LISTINGS)))
        .withColumn("guest_id", (F.col("booking_id") % F.lit(900000) + 100000))
        .withColumn("day_offset", day_offset)
        .withColumn("checkin_date", F.date_add(F.lit(START_DATE.isoformat()).cast("date"), F.col("day_offset")))
        .drop("day_offset")
        .withColumn("checkin_month", F.date_format(F.col("checkin_date"), "yyyy-MM"))
        .withColumn("nights", (F.col("booking_id") % F.lit(14) + 1).cast("int"))
        .withColumn("amount", F.round((F.rand(seed=29) * F.lit(2400.0)) + F.lit(80.0), 2))
        .withColumn("status", F.element_at(F.array(*[F.lit(s) for s in BOOKING_STATUSES]), (F.col("booking_id") % F.lit(len(BOOKING_STATUSES)) + 1).cast("int")))
        .select("booking_id", "listing_id", "guest_id", "checkin_date", "checkin_month", "nights", "amount", "status")
    )


def gen_reviews(spark: SparkSession, rows: int) -> DataFrame:
    base = spark.range(0, rows).withColumnRenamed("id", "review_id")
    day_offset = ((F.col("review_id") * F.lit(97007)) % F.lit(NUM_DAYS)).cast("int")
    return (
        base.withColumn("listing_id", (F.col("review_id") % F.lit(NUM_LISTINGS)))
        .withColumn("reviewer_id", (F.col("review_id") % F.lit(2000000) + 1000000))
        .withColumn("day_offset", day_offset)
        .withColumn("review_date", F.date_add(F.lit(START_DATE.isoformat()).cast("date"), F.col("day_offset")))
        .drop("day_offset")
        .withColumn("review_month", F.date_format(F.col("review_date"), "yyyy-MM"))
        .withColumn("rating", (F.col("review_id") % F.lit(5) + 1).cast("int"))
        .select("review_id", "listing_id", "reviewer_id", "review_date", "review_month", "rating")
    )


def gen_listings(spark: SparkSession) -> DataFrame:
    base = spark.range(0, NUM_LISTINGS).withColumnRenamed("id", "listing_id")
    return (
        base.withColumn("neighbourhood", F.element_at(F.array(*[F.lit(n) for n in NEIGHBOURHOODS]), (F.col("listing_id") % F.lit(len(NEIGHBOURHOODS)) + 1).cast("int")))
        .withColumn("room_type", F.element_at(F.array(*[F.lit(r) for r in ROOM_TYPES]), (F.col("listing_id") % F.lit(len(ROOM_TYPES)) + 1).cast("int")))
        .withColumn("price", F.round((F.rand(seed=71) * F.lit(600.0)) + F.lit(45.0), 2))
        .withColumn("minimum_nights", (F.col("listing_id") % F.lit(7) + 1).cast("int"))
        .select("listing_id", "neighbourhood", "room_type", "price", "minimum_nights")
    )


def _write_partitioned(df: DataFrame, out: str, part_col: str, sort_cols: list[str], rows: int) -> None:
    """Write partitioned, compacted, sorted Parquet (the optimized layout)."""
    files = _files_for(rows)
    _log(f"writing {out} partitioned by {part_col}, ~{files} file(s)/partition, sorted by {sort_cols}")
    (
        df.repartition(NUM_MONTHS * files, part_col)
        .sortWithinPartitions(*sort_cols)
        .write.mode("overwrite").partitionBy(part_col).parquet(out)
    )


def run(spark: SparkSession, out_base: str, target_gb: float, domain: str) -> None:
    out_base = out_base.rstrip("/")

    def txn_rows() -> int:
        return int(target_gb * 1e9 * DOMAIN_SHARE["transactions"] / BYTES_PER_ROW["transactions"])

    def booking_rows() -> int:
        return int(target_gb * 1e9 * DOMAIN_SHARE["bookings"] / BYTES_PER_ROW["bookings"])

    def review_rows() -> int:
        return int(target_gb * 1e9 * DOMAIN_SHARE["reviews"] / BYTES_PER_ROW["reviews"])

    if domain in ("all", "transactions"):
        rows = txn_rows()
        _log(f"transactions target rows: {rows:,} (~{target_gb * DOMAIN_SHARE['transactions']:.0f} GB)")
        _write_partitioned(gen_transactions(spark, rows), f"{out_base}/transactions", "txn_month", ["txn_date", "status", "amount"], rows)

    if domain in ("all", "bookings"):
        rows = booking_rows()
        _log(f"bookings target rows: {rows:,} (~{target_gb * DOMAIN_SHARE['bookings']:.0f} GB)")
        _write_partitioned(gen_bookings(spark, rows), f"{out_base}/bookings", "checkin_month", ["checkin_date", "status", "amount"], rows)

    if domain in ("all", "reviews"):
        rows = review_rows()
        _log(f"reviews target rows: {rows:,} (~{target_gb * DOMAIN_SHARE['reviews']:.0f} GB)")
        _write_partitioned(gen_reviews(spark, rows), f"{out_base}/reviews", "review_month", ["review_date", "rating"], rows)

    if domain in ("all", "listings"):
        _log(f"listings dimension rows: {NUM_LISTINGS:,}")
        gen_listings(spark).coalesce(8).sortWithinPartitions("listing_id").write.mode("overwrite").parquet(f"{out_base}/listings")

    _log(f"=== SCALE GENERATION COMPLETE: ~{target_gb:.0f} GB written under {out_base}/ ===")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate 250+ GB of partitioned Parquet to S3 on EMR.")
    parser.add_argument("--out", required=True, help="s3:// base to write to, e.g. s3://BUCKET/scale")
    parser.add_argument("--target-gb", type=float, default=250.0, help="Approx total on-disk GB across the four domains.")
    parser.add_argument("--domain", default="all", choices=["all", "transactions", "bookings", "reviews", "listings"])
    return parser.parse_args(argv)


def main(argv: list[str]) -> None:
    args = _parse_args(argv)
    spark = (
        SparkSession.builder.appName("scale-generate-emr")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", "512")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        run(spark, args.out, args.target_gb, args.domain)
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
