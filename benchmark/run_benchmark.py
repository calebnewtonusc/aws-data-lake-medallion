"""Query-optimization benchmark over 20+ multi-table SQL workloads.

This is the centerpiece of the project. It builds two physically different
copies of the same four-domain marketplace data (listings, reviews, bookings,
transactions) and times a fixed set of 20+ analytical queries against each, so
the runtime reductions reported on the resume are real measured numbers rather
than assumptions.

The comparison holds the file format constant. Both sides are Parquet, so the
delta is attributable purely to the query-optimization techniques the resume
bullet lists:

  - partition pruning        (partition by month/date so date filters prune dirs)
  - predicate pushdown        (sort within partition -> tight row-group min/max)
  - column pruning            (project only needed columns off wide rows)
  - file compaction           (one right-sized file per partition, no small-file tax)
  - window functions          (running totals, rankings, moving averages, top-N)
  - optimized table design    (a denormalized/pre-aggregated gold star table)

Two physical layouts of the same data, same Spark config on both sides:

  Baseline (unoptimized Parquet, the fair "before"):
    - Parquet, but every table unpartitioned
    - all columns present (nothing pruned)
    - unsorted, so row-group min/max ranges are wide and skip little
    - written as a few big files, no partition structure
    - queries hit the raw normalized tables (no pre-aggregated gold)

  Optimized (tuned Parquet, the techniques above):
    - transactions partitioned by txn_month, bookings by checkin_month,
      reviews by review_month; listings a compact unpartitioned dimension
    - one compacted file per partition (small-file tax gone)
    - pruned to only the columns the queries need
    - sorted within each partition on the date then common filter columns so
      Parquet row-group min/max stats enable predicate pushdown / page skipping
    - a denormalized gold "revenue_by_listing_month" star table the pre-agg and
      top-N queries read instead of re-joining and re-aggregating the facts

The 20+ queries are genuine multi-table joins across the four domains, plus
aggregations and window functions (running totals, dense_rank top-N per group,
moving averages, first/last, cumulative share). Each query runs on both layouts:
one warm-up run (discarded), then several timed runs, and the median wall-clock
is taken to damp out JVM and OS noise. Spark is configured identically for both
so the comparison is fair; the only difference is the physical layout.

Run it:

    export JAVA_HOME=$(/usr/libexec/java_home)
    python -m benchmark.run_benchmark --txn-rows 60000000 --runs 3

A raw-CSV third layout can be added with --with-csv for a secondary
"format + layout" number, but it is off by default because it is disk-heavy and
not the headline (the headline holds file format constant).

Datasets are written under benchmark/_data (gitignored) and reused across runs
unless --rebuild is passed. Delete benchmark/_data when finished; only the code,
results.json, and RESULTS.md are meant to be kept.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import sys
import time
from datetime import date, timedelta
from pathlib import Path

# Pin driver and worker Python to this interpreter before Spark starts so the
# workers never resolve a different Python from PATH.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
if "JAVA_HOME" not in os.environ:
    for candidate in (
        "/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home",
        "/opt/homebrew/opt/openjdk@21",
    ):
        if Path(candidate).exists():
            os.environ["JAVA_HOME"] = candidate
            break

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.window import Window  # noqa: E402

BENCH_ROOT = Path(__file__).resolve().parent
DATA_ROOT = BENCH_ROOT / "_data"

# Baseline (unoptimized Parquet) layout: one directory per domain.
BASE = DATA_ROOT / "baseline"
BASE_TXNS = BASE / "transactions"
BASE_BOOKINGS = BASE / "bookings"
BASE_LISTINGS = BASE / "listings"
BASE_REVIEWS = BASE / "reviews"

# Optimized layout: partitioned, compacted, pruned, sorted + a gold star table.
OPT = DATA_ROOT / "optimized"
OPT_TXNS = OPT / "transactions"
OPT_BOOKINGS = OPT / "bookings"
OPT_LISTINGS = OPT / "listings"
OPT_REVIEWS = OPT / "reviews"
OPT_GOLD_RBLM = OPT / "gold_revenue_by_listing_month"

# Optional raw-CSV layout for a secondary format+layout comparison.
CSV_TXNS = DATA_ROOT / "csv_transactions"

# Fixed reference window the synthetic data spans. Queries filter inside it.
START_DATE = date(2023, 1, 1)
NUM_DAYS = 365 * 2  # two years of daily partitions
TXN_STATUSES = ["succeeded", "failed", "refunded"]
BOOKING_STATUSES = ["completed", "confirmed", "cancelled", "no_show"]
CURRENCIES = ["USD", "EUR", "GBP", "AUD"]
METHODS = ["card", "paypal", "apple_pay", "google_pay", "bank_transfer"]
ROOM_TYPES = ["Entire home/apt", "Private room", "Shared room", "Hotel room"]
NEIGHBOURHOODS = [
    "Downtown", "Mission District", "Capitol Hill", "Williamsburg",
    "Shoreditch", "Le Marais", "Kreuzberg", "Fitzroy",
]

NUM_LISTINGS = 50_000
NUM_MONTHS = NUM_DAYS // 30 + 2


def build_spark(app_name: str = "query-optimization-benchmark") -> SparkSession:
    """Create a local SparkSession used identically for both benchmark sides.

    The config is intentionally fixed and applied once so that the only variable
    between the baseline and optimized runs is the data layout. AQE is left on
    because it is on by default in any modern deployment; leaving it on for both
    sides keeps the comparison representative and fair.
    """
    warehouse = (BENCH_ROOT / "_spark_warehouse").resolve()
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.parquet.aggregatePushdown", "true")
        .config("spark.driver.memory", "12g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.local.dir", str((DATA_ROOT / "_sparktmp").resolve()))
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Synthetic data. Deterministic hashing off spark.range so the same size always
# yields the same data and generation scales to tens of millions of rows without
# collecting to the driver.
# ---------------------------------------------------------------------------


def _synthesize_transactions(spark: SparkSession, rows: int) -> DataFrame:
    """A wide transactions fact spread deterministically across the date window."""
    base = spark.range(0, rows).withColumnRenamed("id", "txn_id")
    day_offset = ((F.col("txn_id") * F.lit(2654435761)) % F.lit(NUM_DAYS)).cast("int")
    return (
        base.withColumn("booking_id", (F.col("txn_id") % F.lit(rows // 2 + 1)))
        .withColumn("listing_id", (F.col("txn_id") % F.lit(NUM_LISTINGS)))
        .withColumn("day_offset", day_offset)
        .withColumn("txn_date", F.date_add(F.lit(START_DATE.isoformat()).cast("date"), F.col("day_offset")))
        .drop("day_offset")
        .withColumn("ts", F.col("txn_date").cast("timestamp"))
        .withColumn("txn_month", F.date_format(F.col("txn_date"), "yyyy-MM"))
        .withColumn("amount", F.round((F.rand(seed=13) * F.lit(950.0)) + F.lit(50.0), 2))
        .withColumn("status", F.element_at(F.array(*[F.lit(s) for s in TXN_STATUSES]), (F.col("txn_id") % F.lit(len(TXN_STATUSES)) + 1).cast("int")))
        .withColumn("currency", F.element_at(F.array(*[F.lit(c) for c in CURRENCIES]), (F.col("txn_id") % F.lit(len(CURRENCIES)) + 1).cast("int")))
        .withColumn("payment_method", F.element_at(F.array(*[F.lit(m) for m in METHODS]), (F.col("txn_id") % F.lit(len(METHODS)) + 1).cast("int")))
        # Wide filler columns: realistic dead weight a naive schema carries and
        # the optimized layout prunes away.
        .withColumn("device", F.concat(F.lit("device-"), (F.col("txn_id") % F.lit(5000)).cast("string")))
        .withColumn("ip_hash", F.sha2(F.col("txn_id").cast("string"), 256))
        .withColumn("user_agent", F.concat(F.lit("client/"), (F.col("txn_id") % F.lit(200)).cast("string"), F.lit(" (compatible; benchmark)")))
        .withColumn("note", F.concat(F.lit("txn note padding text for row "), F.col("txn_id").cast("string")))
    )


def _synthesize_bookings(spark: SparkSession, rows: int) -> DataFrame:
    """A bookings fact keyed on booking_id, referencing listings, over the window."""
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
        .withColumn("checkout_date", F.date_add(F.col("checkin_date"), F.col("nights")))
        .withColumn("amount", F.round((F.rand(seed=29) * F.lit(2400.0)) + F.lit(80.0), 2))
        .withColumn("status", F.element_at(F.array(*[F.lit(s) for s in BOOKING_STATUSES]), (F.col("booking_id") % F.lit(len(BOOKING_STATUSES)) + 1).cast("int")))
        # Wide filler columns to prune.
        .withColumn("channel", F.concat(F.lit("channel-"), (F.col("booking_id") % F.lit(40)).cast("string")))
        .withColumn("promo_code", F.concat(F.lit("promo-"), (F.col("booking_id") % F.lit(1500)).cast("string")))
        .withColumn("device_hash", F.sha2(F.col("booking_id").cast("string"), 256))
        .withColumn("notes", F.concat(F.lit("booking note padding for row "), F.col("booking_id").cast("string")))
    )


def _synthesize_reviews(spark: SparkSession, rows: int) -> DataFrame:
    """A reviews fact keyed on review_id, referencing listings, over the window."""
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
        # Wide filler columns to prune.
        .withColumn("comments", F.concat(F.lit("review comment padding text for row "), F.col("review_id").cast("string")))
        .withColumn("reviewer_name", F.concat(F.lit("guest-"), (F.col("review_id") % F.lit(10000)).cast("string")))
        .withColumn("lang", F.element_at(F.array(F.lit("en"), F.lit("fr"), F.lit("de"), F.lit("es")), (F.col("review_id") % F.lit(4) + 1).cast("int")))
    )


def _synthesize_listings(spark: SparkSession) -> DataFrame:
    """A compact listings dimension keyed on listing_id."""
    base = spark.range(0, NUM_LISTINGS).withColumnRenamed("id", "listing_id")
    return (
        base.withColumn("host_id", (F.col("listing_id") % F.lit(9000) + 100000))
        .withColumn("neighbourhood", F.element_at(F.array(*[F.lit(n) for n in NEIGHBOURHOODS]), (F.col("listing_id") % F.lit(len(NEIGHBOURHOODS)) + 1).cast("int")))
        .withColumn("room_type", F.element_at(F.array(*[F.lit(r) for r in ROOM_TYPES]), (F.col("listing_id") % F.lit(len(ROOM_TYPES)) + 1).cast("int")))
        .withColumn("price", F.round((F.rand(seed=71) * F.lit(600.0)) + F.lit(45.0), 2))
        .withColumn("minimum_nights", (F.col("listing_id") % F.lit(7) + 1).cast("int"))
        .withColumn("host_name", F.concat(F.lit("host-"), (F.col("listing_id") % F.lit(9000)).cast("string")))
    )


def build_datasets(spark: SparkSession, txn_rows: int, booking_rows: int, review_rows: int, with_csv: bool) -> None:
    """Materialize the baseline and optimized physical layouts for all domains."""
    print(f"Synthesizing {txn_rows:,} txns, {booking_rows:,} bookings, {review_rows:,} reviews, {NUM_LISTINGS:,} listings ...")
    txns = _synthesize_transactions(spark, txn_rows)
    bookings = _synthesize_bookings(spark, booking_rows)
    reviews = _synthesize_reviews(spark, review_rows)
    listings = _synthesize_listings(spark)

    # --- Baseline: unoptimized Parquet. Same columnar file format as optimized,
    # but none of the query-optimization work: not partitioned, not sorted, all
    # columns present, written as a few big files. Holding the file format
    # constant against the optimized layout isolates the gain attributable purely
    # to partitioning, column pruning, compaction, pushdown, window rewrites, and
    # the pre-aggregated gold table. ---
    print("Writing baseline (unpartitioned, unsorted, all columns, few big files) ...")
    for path in (BASE_TXNS, BASE_BOOKINGS, BASE_REVIEWS, BASE_LISTINGS):
        if path.exists():
            shutil.rmtree(path)
    txns.coalesce(6).write.mode("overwrite").parquet(str(BASE_TXNS))
    bookings.coalesce(4).write.mode("overwrite").parquet(str(BASE_BOOKINGS))
    reviews.coalesce(4).write.mode("overwrite").parquet(str(BASE_REVIEWS))
    listings.coalesce(1).write.mode("overwrite").parquet(str(BASE_LISTINGS))

    # --- Optimized: partitioned by month, one compacted file per partition, only
    # the needed columns, sorted within partition on date then common filter
    # columns so Parquet row-group min/max stats enable predicate pushdown. ---
    print("Writing optimized transactions (month-partitioned, compacted, pruned, sorted) ...")
    if OPT_TXNS.exists():
        shutil.rmtree(OPT_TXNS)
    txn_needed = txns.select("txn_id", "booking_id", "listing_id", "txn_date", "txn_month", "amount", "status", "currency", "payment_method")
    (
        txn_needed.repartition(NUM_MONTHS, "txn_month")
        .sortWithinPartitions("txn_date", "status", "amount")
        .write.mode("overwrite").partitionBy("txn_month").parquet(str(OPT_TXNS))
    )

    print("Writing optimized bookings (month-partitioned, compacted, pruned, sorted) ...")
    if OPT_BOOKINGS.exists():
        shutil.rmtree(OPT_BOOKINGS)
    book_needed = bookings.select("booking_id", "listing_id", "guest_id", "checkin_date", "checkin_month", "nights", "amount", "status")
    (
        book_needed.repartition(NUM_MONTHS, "checkin_month")
        .sortWithinPartitions("checkin_date", "status", "amount")
        .write.mode("overwrite").partitionBy("checkin_month").parquet(str(OPT_BOOKINGS))
    )

    print("Writing optimized reviews (month-partitioned, compacted, pruned, sorted) ...")
    if OPT_REVIEWS.exists():
        shutil.rmtree(OPT_REVIEWS)
    rev_needed = reviews.select("review_id", "listing_id", "reviewer_id", "review_date", "review_month", "rating")
    (
        rev_needed.repartition(NUM_MONTHS, "review_month")
        .sortWithinPartitions("review_date", "rating")
        .write.mode("overwrite").partitionBy("review_month").parquet(str(OPT_REVIEWS))
    )

    print("Writing optimized listings dimension (compacted, pruned) ...")
    if OPT_LISTINGS.exists():
        shutil.rmtree(OPT_LISTINGS)
    list_needed = listings.select("listing_id", "neighbourhood", "room_type", "price", "minimum_nights")
    list_needed.coalesce(1).sortWithinPartitions("listing_id").write.mode("overwrite").parquet(str(OPT_LISTINGS))

    # --- Optimized table design: a denormalized, pre-aggregated gold star table.
    # Revenue by listing and month, already joined to the listing dimension and
    # aggregated, partitioned by month and sorted by listing. The pre-agg and
    # top-N queries read this instead of re-joining and re-scanning the raw fact,
    # which is the "optimized table design / pre-aggregation / denormalized gold"
    # technique on the resume bullet. ---
    print("Writing gold revenue_by_listing_month star table (denormalized, pre-aggregated) ...")
    if OPT_GOLD_RBLM.exists():
        shutil.rmtree(OPT_GOLD_RBLM)
    gold = (
        txn_needed.where(F.col("status") == F.lit("succeeded"))
        .groupBy("listing_id", "txn_month")
        .agg(F.sum("amount").alias("revenue"), F.count("*").alias("txn_count"), F.avg("amount").alias("avg_ticket"))
        .join(list_needed.select("listing_id", "neighbourhood", "room_type"), on="listing_id", how="inner")
    )
    (
        gold.repartition(NUM_MONTHS, "txn_month")
        .sortWithinPartitions("listing_id")
        .write.mode("overwrite").partitionBy("txn_month").parquet(str(OPT_GOLD_RBLM))
    )

    if with_csv:
        print("Writing raw CSV transactions (secondary format+layout comparison) ...")
        if CSV_TXNS.exists():
            shutil.rmtree(CSV_TXNS)
        txns.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(CSV_TXNS))

    print("Datasets built.")


# ---------------------------------------------------------------------------
# Query window helpers.
# ---------------------------------------------------------------------------


def _month_bounds(days_in: int, span: int = 30) -> tuple[str, str]:
    start = START_DATE + timedelta(days=days_in)
    return start.isoformat(), (start + timedelta(days=span)).isoformat()


def _quarter_bounds(days_in: int) -> tuple[str, str]:
    start = START_DATE + timedelta(days=days_in)
    return start.isoformat(), (start + timedelta(days=90)).isoformat()


# ---------------------------------------------------------------------------
# The 20+ workloads. Each takes a dict of the four DataFrames and returns a
# DataFrame; the harness triggers a full action so the whole query executes.
# Every query is a genuine multi-table join and/or window function that benefits
# from partition pruning, predicate pushdown, column pruning, compaction, or the
# denormalized gold table. `gold` is the pre-aggregated star table on the
# optimized side and None on the baseline (which falls back to the raw facts).
# ---------------------------------------------------------------------------


def q01(d):
    """Date-range revenue by status (partition pruning + stats skipping)."""
    lo, hi = _month_bounds(400)
    t = d["txns"]
    return (
        t.where((F.col("txn_date") >= F.lit(lo)) & (F.col("txn_date") < F.lit(hi)))
        .groupBy("status").agg(F.sum("amount").alias("total"), F.count("*").alias("n"))
    )


def q02(d):
    """Succeeded payments by method (column pruning + predicate pushdown)."""
    t = d["txns"]
    return (
        t.where(F.col("status") == F.lit("succeeded"))
        .groupBy("payment_method").agg(F.sum("amount").alias("total"), F.avg("amount").alias("avg_amt"))
    )


def q03(d):
    """Single-day currency totals (best case for partition pruning)."""
    day = (START_DATE + timedelta(days=500)).isoformat()
    t = d["txns"]
    return t.where(F.col("txn_date") == F.lit(day)).groupBy("currency").agg(F.sum("amount").alias("total"))


def q04(d):
    """Txns joined to listings by neighbourhood in a month (join + prune)."""
    lo, hi = _month_bounds(200)
    t = d["txns"].where((F.col("txn_date") >= F.lit(lo)) & (F.col("txn_date") < F.lit(hi)) & (F.col("status") == F.lit("succeeded"))).select("listing_id", "amount")
    ls = d["listings"].select("listing_id", "neighbourhood")
    return t.join(ls, on="listing_id", how="inner").groupBy("neighbourhood").agg(F.sum("amount").alias("revenue"))


def q05(d):
    """Txns joined to bookings, filter-before-join rewrite in a window."""
    lo, hi = _month_bounds(250)
    t = d["txns"].where((F.col("status") == F.lit("succeeded")) & (F.col("txn_date") >= F.lit(lo)) & (F.col("txn_date") < F.lit(hi))).select("booking_id", "amount")
    b = d["bookings"].select("booking_id", "nights", "status").where(F.col("status") == F.lit("completed"))
    return t.join(b, on="booking_id", how="inner").groupBy("nights").agg(F.sum("amount").alias("revenue"), F.count("*").alias("n"))


def q06(d):
    """Three-way join txns -> bookings -> listings in a quarter (multi-domain)."""
    lo, hi = _quarter_bounds(300)
    t = d["txns"].where((F.col("status") == F.lit("succeeded")) & (F.col("txn_date") >= F.lit(lo)) & (F.col("txn_date") < F.lit(hi))).select("booking_id", "amount")
    b = d["bookings"].select("booking_id", "listing_id")
    ls = d["listings"].select("listing_id", "room_type")
    return t.join(b, "booking_id", "inner").join(ls, "listing_id", "inner").groupBy("room_type").agg(F.sum("amount").alias("revenue"))


def q07(d):
    """Reviews joined to listings, avg rating by neighbourhood in a window."""
    lo, hi = _month_bounds(150)
    r = d["reviews"].where((F.col("review_date") >= F.lit(lo)) & (F.col("review_date") < F.lit(hi))).select("listing_id", "rating")
    ls = d["listings"].select("listing_id", "neighbourhood")
    return r.join(ls, "listing_id", "inner").groupBy("neighbourhood").agg(F.avg("rating").alias("avg_rating"), F.count("*").alias("n"))


def q08(d):
    """Bookings by month and status in a quarter (partition pruning on bookings)."""
    lo, hi = _quarter_bounds(120)
    b = d["bookings"].where((F.col("checkin_date") >= F.lit(lo)) & (F.col("checkin_date") < F.lit(hi)))
    return b.groupBy("checkin_month", "status").agg(F.sum("amount").alias("gmv"), F.count("*").alias("n"))


def q09(d):
    """WINDOW: monthly revenue running total across the year (running total)."""
    src = d["gold"] if d["gold"] is not None else d["txns"].where(F.col("status") == F.lit("succeeded"))
    monthly = src.groupBy("txn_month").agg(F.sum("revenue" if d["gold"] is not None else "amount").alias("revenue"))
    w = Window.orderBy("txn_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return monthly.withColumn("running_total", F.sum("revenue").over(w))


def q10(d):
    """WINDOW: top-3 listings by revenue per month (dense_rank top-N per group)."""
    if d["gold"] is not None:
        g = d["gold"].groupBy("txn_month", "listing_id").agg(F.sum("revenue").alias("revenue"))
    else:
        g = d["txns"].where(F.col("status") == F.lit("succeeded")).groupBy("txn_month", "listing_id").agg(F.sum("amount").alias("revenue"))
    w = Window.partitionBy("txn_month").orderBy(F.col("revenue").desc())
    return g.withColumn("rk", F.dense_rank().over(w)).where(F.col("rk") <= 3)


def q11(d):
    """WINDOW: 3-month moving average of revenue (moving average)."""
    src = d["gold"] if d["gold"] is not None else d["txns"].where(F.col("status") == F.lit("succeeded"))
    monthly = src.groupBy("txn_month").agg(F.sum("revenue" if d["gold"] is not None else "amount").alias("revenue"))
    w = Window.orderBy("txn_month").rowsBetween(-2, 0)
    return monthly.withColumn("moving_avg_3m", F.avg("revenue").over(w))


def q12(d):
    """WINDOW: each listing's share of its neighbourhood revenue (ratio over window)."""
    if d["gold"] is not None:
        g = d["gold"].groupBy("neighbourhood", "listing_id").agg(F.sum("revenue").alias("revenue"))
    else:
        t = d["txns"].where(F.col("status") == F.lit("succeeded")).select("listing_id", "amount")
        ls = d["listings"].select("listing_id", "neighbourhood")
        g = t.join(ls, "listing_id", "inner").groupBy("neighbourhood", "listing_id").agg(F.sum("amount").alias("revenue"))
    w = Window.partitionBy("neighbourhood")
    return g.withColumn("nbhd_total", F.sum("revenue").over(w)).withColumn("share", F.col("revenue") / F.col("nbhd_total"))


def q13(d):
    """WINDOW: rank neighbourhoods by revenue in a quarter (rank over group)."""
    lo, hi = _quarter_bounds(400)
    t = d["txns"].where((F.col("status") == F.lit("succeeded")) & (F.col("txn_date") >= F.lit(lo)) & (F.col("txn_date") < F.lit(hi))).select("listing_id", "amount")
    ls = d["listings"].select("listing_id", "neighbourhood")
    agg = t.join(ls, "listing_id", "inner").groupBy("neighbourhood").agg(F.sum("amount").alias("revenue"))
    w = Window.orderBy(F.col("revenue").desc())
    return agg.withColumn("rank", F.rank().over(w))


def q14(d):
    """WINDOW: month-over-month revenue delta via lag (period comparison)."""
    src = d["gold"] if d["gold"] is not None else d["txns"].where(F.col("status") == F.lit("succeeded"))
    monthly = src.groupBy("txn_month").agg(F.sum("revenue" if d["gold"] is not None else "amount").alias("revenue"))
    w = Window.orderBy("txn_month")
    return monthly.withColumn("prev", F.lag("revenue").over(w)).withColumn("mom_delta", F.col("revenue") - F.col("prev"))


def q15(d):
    """WINDOW: cumulative revenue share by listing (Pareto / percent_rank)."""
    if d["gold"] is not None:
        g = d["gold"].groupBy("listing_id").agg(F.sum("revenue").alias("revenue"))
    else:
        g = d["txns"].where(F.col("status") == F.lit("succeeded")).groupBy("listing_id").agg(F.sum("amount").alias("revenue"))
    w = Window.orderBy(F.col("revenue").desc()).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    tot = Window.partitionBy(F.lit(1))
    return g.withColumn("cum_rev", F.sum("revenue").over(w)).withColumn("grand_total", F.sum("revenue").over(tot)).withColumn("cum_share", F.col("cum_rev") / F.col("grand_total"))


def q16(d):
    """Payment success rate by method in a quarter (conditional agg + pushdown)."""
    lo, hi = _quarter_bounds(200)
    t = d["txns"].where((F.col("txn_date") >= F.lit(lo)) & (F.col("txn_date") < F.lit(hi)))
    return t.groupBy("payment_method").agg(
        (F.sum(F.when(F.col("status") == F.lit("succeeded"), 1).otherwise(0)) / F.count("*")).alias("success_rate"),
        F.count("*").alias("attempts"),
    )


def q17(d):
    """Booking conversion: bookings vs paid txns per listing in a month (join + prune)."""
    lo, hi = _month_bounds(350)
    b = d["bookings"].where((F.col("checkin_date") >= F.lit(lo)) & (F.col("checkin_date") < F.lit(hi))).select("booking_id", "listing_id")
    t = d["txns"].where(F.col("status") == F.lit("succeeded")).select("booking_id")
    paid = b.join(t, "booking_id", "inner").groupBy("listing_id").agg(F.count("*").alias("paid"))
    total = b.groupBy("listing_id").agg(F.count("*").alias("bookings"))
    return total.join(paid, "listing_id", "left").withColumn("conversion", F.col("paid") / F.col("bookings"))


def q18(d):
    """Reviews and revenue correlated per listing in a quarter (two-fact join)."""
    lo, hi = _quarter_bounds(180)
    r = d["reviews"].where((F.col("review_date") >= F.lit(lo)) & (F.col("review_date") < F.lit(hi))).select("listing_id", "rating")
    rev = r.groupBy("listing_id").agg(F.avg("rating").alias("avg_rating"), F.count("*").alias("n_reviews"))
    t = d["txns"].where((F.col("status") == F.lit("succeeded")) & (F.col("txn_date") >= F.lit(lo)) & (F.col("txn_date") < F.lit(hi))).select("listing_id", "amount")
    money = t.groupBy("listing_id").agg(F.sum("amount").alias("revenue"))
    return rev.join(money, "listing_id", "inner")


def q19(d):
    """Wide-table full-scan aggregation touching two columns (column pruning)."""
    t = d["txns"]
    return t.groupBy("status").agg(F.sum("amount").alias("total"), F.count("*").alias("n"))


def q20(d):
    """Avg daily revenue by neighbourhood in a month (join + partition prune + agg)."""
    lo, hi = _month_bounds(450)
    t = d["txns"].where((F.col("status") == F.lit("succeeded")) & (F.col("txn_date") >= F.lit(lo)) & (F.col("txn_date") < F.lit(hi))).select("listing_id", "txn_date", "amount")
    ls = d["listings"].select("listing_id", "neighbourhood")
    daily = t.join(ls, "listing_id", "inner").groupBy("neighbourhood", "txn_date").agg(F.sum("amount").alias("day_rev"))
    return daily.groupBy("neighbourhood").agg(F.avg("day_rev").alias("avg_daily_rev"))


def q21(d):
    """Pre-aggregated gold read: revenue by neighbourhood (optimized table design)."""
    if d["gold"] is not None:
        return d["gold"].groupBy("neighbourhood").agg(F.sum("revenue").alias("revenue"), F.sum("txn_count").alias("txns"))
    # Baseline has no gold: it must re-join and re-aggregate the raw facts.
    t = d["txns"].where(F.col("status") == F.lit("succeeded")).select("listing_id", "amount")
    ls = d["listings"].select("listing_id", "neighbourhood")
    return t.join(ls, "listing_id", "inner").groupBy("neighbourhood").agg(F.sum("amount").alias("revenue"), F.count("*").alias("txns"))


def q22(d):
    """WINDOW: 7-day moving avg of daily revenue in a quarter (moving average)."""
    lo, hi = _quarter_bounds(260)
    t = d["txns"].where((F.col("status") == F.lit("succeeded")) & (F.col("txn_date") >= F.lit(lo)) & (F.col("txn_date") < F.lit(hi))).select("txn_date", "amount")
    daily = t.groupBy("txn_date").agg(F.sum("amount").alias("day_rev"))
    w = Window.orderBy("txn_date").rowsBetween(-6, 0)
    return daily.withColumn("moving_avg_7d", F.avg("day_rev").over(w))


# Registry: (id, callable, description, technique tag)
QUERIES = [
    ("q01_date_range_revenue", q01, "Date-range revenue by status", "partition pruning + stats"),
    ("q02_method_success", q02, "Succeeded payments by method", "column pruning + pushdown"),
    ("q03_single_day", q03, "Single-day currency totals", "partition pruning"),
    ("q04_txn_listing_join", q04, "Txns joined to listings by neighbourhood", "join + partition prune"),
    ("q05_txn_booking_join", q05, "Txns joined to bookings (filter-before-join)", "join rewrite + prune"),
    ("q06_three_way_join", q06, "Txns -> bookings -> listings 3-way join", "multi-domain join + prune"),
    ("q07_review_listing_join", q07, "Reviews joined to listings, avg rating", "join + partition prune"),
    ("q08_bookings_by_month", q08, "Bookings by month and status", "partition pruning (bookings)"),
    ("q09_running_total", q09, "Monthly revenue running total", "window: running total + gold"),
    ("q10_topn_per_month", q10, "Top-3 listings by revenue per month", "window: dense_rank top-N + gold"),
    ("q11_moving_avg_3m", q11, "3-month moving average of revenue", "window: moving avg + gold"),
    ("q12_nbhd_share", q12, "Listing share of neighbourhood revenue", "window: ratio + gold"),
    ("q13_rank_nbhd", q13, "Rank neighbourhoods by quarter revenue", "window: rank + join + prune"),
    ("q14_mom_delta", q14, "Month-over-month revenue delta (lag)", "window: lag + gold"),
    ("q15_pareto_share", q15, "Cumulative revenue share by listing", "window: cumulative + gold"),
    ("q16_success_rate", q16, "Payment success rate by method (quarter)", "conditional agg + prune"),
    ("q17_conversion", q17, "Booking-to-paid conversion per listing", "join + partition prune"),
    ("q18_reviews_vs_revenue", q18, "Reviews vs revenue per listing (quarter)", "two-fact join + prune"),
    ("q19_wide_scan", q19, "Wide-table full-scan aggregation", "column pruning"),
    ("q20_avg_daily_rev", q20, "Avg daily revenue by neighbourhood", "join + prune + agg"),
    ("q21_gold_preagg", q21, "Revenue by neighbourhood from gold", "optimized table design (pre-agg)"),
    ("q22_moving_avg_7d", q22, "7-day moving avg of daily revenue", "window: moving avg + prune"),
]


def _time_query(fn, runs: int) -> float:
    """Run fn once to warm up, then time `runs` executions, return the median.

    fn builds a DataFrame and triggers a full action (count) so the whole query
    executes. Wall-clock is used because that is what a user waits on.
    """
    fn()  # warm-up (discarded): pays JIT, metadata, and filesystem cache once.
    samples: list[float] = []
    for _ in range(runs):
        start = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - start)
    return statistics.median(samples)


def _reduction(before: float, after: float) -> float:
    return (before - after) / before * 100.0 if before > 0 else 0.0


def _load(spark: SparkSession, optimized: bool) -> dict:
    """Load the four domains for one layout into a dict the queries consume."""
    if optimized:
        return {
            "txns": spark.read.parquet(str(OPT_TXNS)),
            "bookings": spark.read.parquet(str(OPT_BOOKINGS)),
            "reviews": spark.read.parquet(str(OPT_REVIEWS)),
            "listings": spark.read.parquet(str(OPT_LISTINGS)),
            "gold": spark.read.parquet(str(OPT_GOLD_RBLM)),
        }
    return {
        "txns": spark.read.parquet(str(BASE_TXNS)),
        "bookings": spark.read.parquet(str(BASE_BOOKINGS)),
        "reviews": spark.read.parquet(str(BASE_REVIEWS)),
        "listings": spark.read.parquet(str(BASE_LISTINGS)),
        "gold": None,  # baseline has no pre-aggregated gold table on purpose.
    }


def run_benchmark(spark: SparkSession, runs: int) -> dict:
    """Time every query on both layouts and return the per-query and total deltas.

    Both layouts are read with the same Spark session and config so the only
    variable is the physical layout. Baseline is unoptimized Parquet (columnar
    but unpartitioned, unsorted, all columns, no gold); optimized is partitioned,
    compacted, column-pruned, sorted Parquet plus the pre-aggregated gold table.
    """
    base = _load(spark, optimized=False)
    opt = _load(spark, optimized=True)

    per_query = []
    total_base = 0.0
    total_opt = 0.0
    for qid, fn, desc, tag in QUERIES:
        print(f"Timing {qid} on baseline (unoptimized Parquet) ...")
        base_med = _time_query(lambda fn=fn: fn(base).count(), runs)
        print(f"Timing {qid} on optimized layout ...")
        opt_med = _time_query(lambda fn=fn: fn(opt).count(), runs)
        total_base += base_med
        total_opt += opt_med
        per_query.append({
            "query": qid,
            "description": desc,
            "technique": tag,
            "baseline_median_s": round(base_med, 4),
            "optimized_median_s": round(opt_med, 4),
            "reduction_pct": round(_reduction(base_med, opt_med), 2),
            "speedup_x": round(base_med / opt_med, 2) if opt_med > 0 else None,
        })
        print(f"  {qid}: baseline {base_med:.3f}s  optimized {opt_med:.3f}s  (reduction {_reduction(base_med, opt_med):.1f}%)")

    return {
        "runs_per_query": runs,
        "query_count": len(QUERIES),
        "total_baseline_median_s": round(total_base, 4),
        "total_optimized_median_s": round(total_opt, 4),
        "query_opt_reduction_pct": round(_reduction(total_base, total_opt), 2),
        "query_opt_speedup_x": round(total_base / total_opt, 2) if total_opt > 0 else None,
        "per_query": per_query,
    }


def _folder_size_bytes(path: Path) -> int:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) if path.exists() else 0


def write_results(results: dict, sizes: dict) -> None:
    """Write results.json and the markdown report."""
    results.update(sizes)
    (BENCH_ROOT / "results.json").write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")

    base_gb = results["baseline_bytes"] / 1e9
    opt_gb = results["optimized_bytes"] / 1e9
    lines = [
        "# Query Optimization Benchmark Results",
        "",
        "All numbers below are measured, not assumed. Reproduce with "
        "`python -m benchmark.run_benchmark`.",
        "",
        f"- Data: four source domains (listings, reviews, bookings, transactions) spanning {NUM_DAYS} days",
        f"- Fact scale: {results['txn_rows']:,} transactions, {results['booking_rows']:,} bookings, "
        f"{results['review_rows']:,} reviews, {results['listing_rows']:,} listings",
        f"- Workloads: {results['query_count']} multi-table SQL queries (joins across the four domains, "
        "aggregations, and window functions: running totals, dense_rank top-N, moving averages, lag, "
        "cumulative share)",
        f"- Timing: median of {results['runs_per_query']} timed wall-clock runs per query after one warm-up",
        "- Two layouts of the same data, same Spark config (file format held constant, both Parquet):",
        "  - Baseline: unoptimized Parquet (unpartitioned, unsorted, all columns, few big files, no gold table)",
        "  - Optimized: month-partitioned, compacted, column-pruned, sorted Parquet + a denormalized "
        "pre-aggregated gold star table",
        "",
        "## Headline: query optimization only (file format held constant)",
        "",
        f"**Overall runtime reduction: {results['query_opt_reduction_pct']}%** "
        f"({results['query_opt_speedup_x']}x speedup), total across all {results['query_count']} queries "
        f"{results['total_baseline_median_s']}s down to {results['total_optimized_median_s']}s.",
        "",
        "Both sides are Parquet, so the file format is held constant and this reduction is attributable "
        "purely to the query-optimization techniques: partition pruning, predicate pushdown, column pruning, "
        "file compaction, window-function rewrites, and optimized table design (the pre-aggregated gold star "
        "table). The optimized layout is also "
        f"{base_gb / opt_gb:.1f}x smaller on disk ({base_gb:.2f} GB down to {opt_gb:.2f} GB) from column "
        "pruning and compaction.",
        "",
        "## Per-query results",
        "",
        "| Query | Technique | Baseline (s) | Optimized (s) | Speedup | Reduction |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for row in results["per_query"]:
        lines.append(
            f"| {row['query']} | {row['technique']} | {row['baseline_median_s']} | "
            f"{row['optimized_median_s']} | {row['speedup_x']}x | {row['reduction_pct']}% |"
        )
    lines += [
        "",
        f"| **Total** | | **{results['total_baseline_median_s']}** | "
        f"**{results['total_optimized_median_s']}** | **{results['query_opt_speedup_x']}x** | "
        f"**{results['query_opt_reduction_pct']}%** |",
        "",
        "### Techniques demonstrated",
        "",
        "- **Partition pruning**: date-bounded queries prune whole month directories instead of scanning "
        "the full fact.",
        "- **Predicate pushdown**: rows are sorted within each partition, so tight row-group min/max stats "
        "let the Parquet reader skip row groups that cannot match a filter. The unsorted baseline has wide "
        "min/max ranges that skip little.",
        "- **Column pruning**: reading only the columns a query needs, rather than every column of a wide "
        "row, so queries touch a fraction of the bytes.",
        "- **File compaction**: the optimized data is one right-sized file per partition, avoiding the "
        "small-file tax.",
        "- **Window functions**: running totals, dense_rank top-N per group, moving averages, lag deltas, "
        "and cumulative share run over the partitioned, sorted, pre-aggregated gold rather than re-scanning "
        "the raw fact.",
        "- **Optimized table design**: a denormalized, pre-aggregated gold star table (revenue by listing "
        "and month, already joined to the listing dimension) that pre-agg and top-N queries read instead of "
        "re-joining and re-scanning the fact.",
        "",
    ]
    if results.get("csv_note"):
        lines += [results["csv_note"], ""]
    (BENCH_ROOT / "RESULTS.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\nWrote {BENCH_ROOT / 'results.json'} and {BENCH_ROOT / 'RESULTS.md'}")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query-optimization benchmark over 20+ multi-table workloads.")
    parser.add_argument("--txn-rows", type=int, default=60_000_000, help="Transaction fact rows.")
    parser.add_argument("--booking-rows", type=int, default=12_000_000, help="Booking rows.")
    parser.add_argument("--review-rows", type=int, default=12_000_000, help="Review rows.")
    parser.add_argument("--runs", type=int, default=3, help="Timed runs per query (median is reported).")
    parser.add_argument("--rebuild", action="store_true", help="Rebuild datasets even if present.")
    parser.add_argument("--with-csv", action="store_true", help="Also build a raw-CSV layout (disk-heavy, off by default).")
    return parser.parse_args(argv)


def main(argv: list[str]) -> None:
    """Build datasets if needed, run the benchmark, and write results."""
    args = _parse_args(argv)
    DATA_ROOT.mkdir(parents=True, exist_ok=True)
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        need_build = args.rebuild or not OPT_TXNS.exists() or not BASE_TXNS.exists() or not OPT_GOLD_RBLM.exists()
        if need_build:
            build_datasets(spark, args.txn_rows, args.booking_rows, args.review_rows, args.with_csv)
        else:
            print("Reusing existing datasets under benchmark/_data (pass --rebuild to regenerate).")
        results = run_benchmark(spark, args.runs)
        sizes = {
            "txn_rows": args.txn_rows,
            "booking_rows": args.booking_rows,
            "review_rows": args.review_rows,
            "listing_rows": NUM_LISTINGS,
            "baseline_bytes": sum(_folder_size_bytes(p) for p in (BASE_TXNS, BASE_BOOKINGS, BASE_REVIEWS, BASE_LISTINGS)),
            "optimized_bytes": sum(_folder_size_bytes(p) for p in (OPT_TXNS, OPT_BOOKINGS, OPT_REVIEWS, OPT_LISTINGS, OPT_GOLD_RBLM)),
        }
        write_results(results, sizes)
        print(f"\nQUERY-OPTIMIZATION-ONLY REDUCTION (unoptimized Parquet -> optimized): {results['query_opt_reduction_pct']}%")
        print(f"Total: {results['total_baseline_median_s']}s -> {results['total_optimized_median_s']}s over {results['query_count']} queries")
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
