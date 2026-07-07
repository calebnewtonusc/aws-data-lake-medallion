"""Query-optimization benchmark: three physical layouts, one set of queries.

This is the centerpiece of the project. It builds three physically different
copies of the same synthetic transaction data and times a fixed set of
analytical queries against each, so the runtime reductions reported on the
resume are real measured numbers rather than assumptions.

It reports two clearly separated reductions:

  1. Format + layout (CSV -> optimized Parquet). The whole improvement a raw
     dump gets from being turned into a proper lake. Large, but part of it is
     just the file-format change from row-oriented CSV to columnar Parquet.
  2. Query optimization only (unoptimized Parquet -> optimized Parquet). Both
     sides are Parquet, so the file format is held constant and the delta is
     attributable purely to the query-optimization techniques: partitioning,
     column pruning, file compaction, predicate pushdown, and SQL rewrites.
     This is the honest number for the resume bullet's listed techniques.

The three layouts:

  Baseline A, raw CSV (everything a slow lake gets wrong):
    - a single large CSV, no columnar format, no column statistics
    - unpartitioned, unsorted, all columns present

  Baseline B, unoptimized Parquet (fair "before" for query optimization):
    - Parquet, but unpartitioned
    - all columns present (no column pruning)
    - unsorted, so min/max row-group skipping cannot help
    - written as a few big files with no partition structure

  Optimized, tuned Parquet (the techniques the resume bullet claims):
    - partitioned by transaction month, so date filters prune whole directories
    - compacted into one right-sized file per partition (small-file tax gone)
    - pruned to only the columns the queries need
    - sorted within each partition on transaction date and the common filter
      columns so Parquet row-group min/max stats enable predicate pushdown

The queries deliberately exercise each technique:
  q1 date-range aggregation      -> partition pruning + stats skipping
  q2 status filter + group by     -> column pruning + predicate pushdown
  q3 single-day point lookup      -> partition pruning to one directory
  q4 join bookings to txns (rewrite: filter-before-join, no SELECT *)
  q5 wide full-scan aggregation   -> column pruning benefit on a heavy scan

Each query runs on every layout: one warm-up run (discarded) then several
timed runs, and the median wall-clock is taken to damp out JVM and OS noise.
Spark is configured identically for all three so the comparison is fair; the
only difference is the physical layout of the data.

Run it:

    export JAVA_HOME=$(/usr/libexec/java_home)
    python -m benchmark.run_benchmark --rows 20000000 --runs 3

Datasets are written under benchmark/_data (gitignored) and reused across
runs unless --rebuild is passed.
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

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

BENCH_ROOT = Path(__file__).resolve().parent
DATA_ROOT = BENCH_ROOT / "_data"
BASELINE_CSV = DATA_ROOT / "baseline_csv"
BASELINE_PARQUET = DATA_ROOT / "baseline_parquet"
OPTIMIZED_TXNS = DATA_ROOT / "optimized_transactions"
OPTIMIZED_BOOKINGS = DATA_ROOT / "optimized_bookings"

# Fixed reference window the synthetic data spans. Queries filter inside it.
START_DATE = date(2023, 1, 1)
NUM_DAYS = 365 * 2  # two years of daily partitions
STATUSES = ["succeeded", "failed", "refunded"]
CURRENCIES = ["USD", "EUR", "GBP", "AUD"]
METHODS = ["card", "paypal", "apple_pay", "google_pay", "bank_transfer"]


def build_spark(app_name: str = "query-optimization-benchmark") -> SparkSession:
    """Create a local SparkSession used identically for both benchmark sides.

    The config is intentionally fixed and applied once so that the only
    variable between the baseline and optimized runs is the data layout. AQE
    is left on because it is on by default in any modern deployment; leaving it
    on for both sides keeps the comparison representative and fair.
    """
    warehouse = (BENCH_ROOT / "_spark_warehouse").resolve()
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.parquet.aggregatePushdown", "true")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )


def _synthesize(spark: SparkSession, rows: int) -> DataFrame:
    """Build a synthetic transactions DataFrame of the requested row count.

    Uses spark.range plus deterministic hashing so the same row count always
    yields the same data, and so generation scales to tens of millions of rows
    without collecting to the driver. Columns mirror the transactions dataset
    plus a few wide filler columns so column pruning has something to prune.
    """
    base = spark.range(0, rows).withColumnRenamed("id", "txn_id")
    # Spread rows deterministically across the daily partition window.
    day_offset = ((F.col("txn_id") * F.lit(2654435761)) % F.lit(NUM_DAYS)).cast("int")
    df = (
        base.withColumn("booking_id", (F.col("txn_id") % F.lit(rows // 3 + 1)))
        .withColumn("day_offset", day_offset)
        .withColumn("txn_date", F.date_add(F.lit(START_DATE.isoformat()).cast("date"), F.col("day_offset")))
        .drop("day_offset")
        .withColumn("ts", F.col("txn_date").cast("timestamp"))
        .withColumn("txn_month", F.date_format(F.col("txn_date"), "yyyy-MM"))
        .withColumn("amount", F.round((F.rand(seed=13) * F.lit(950.0)) + F.lit(50.0), 2))
        .withColumn("status", F.element_at(F.array(*[F.lit(s) for s in STATUSES]), (F.col("txn_id") % F.lit(len(STATUSES)) + 1).cast("int")))
        .withColumn("currency", F.element_at(F.array(*[F.lit(c) for c in CURRENCIES]), (F.col("txn_id") % F.lit(len(CURRENCIES)) + 1).cast("int")))
        .withColumn("payment_method", F.element_at(F.array(*[F.lit(m) for m in METHODS]), (F.col("txn_id") % F.lit(len(METHODS)) + 1).cast("int")))
        # Wide filler columns: realistic dead weight a naive schema carries and
        # the optimized layout prunes away.
        .withColumn("device", F.concat(F.lit("device-"), (F.col("txn_id") % F.lit(5000)).cast("string")))
        .withColumn("ip_hash", F.sha2(F.col("txn_id").cast("string"), 256))
        .withColumn("user_agent", F.concat(F.lit("client/"), (F.col("txn_id") % F.lit(200)).cast("string"), F.lit(" (compatible; benchmark)")))
        .withColumn("note", F.concat(F.lit("txn note padding text for row "), F.col("txn_id").cast("string")))
    )
    return df


def build_datasets(spark: SparkSession, rows: int) -> None:
    """Materialize the baseline and optimized physical layouts on disk."""
    print(f"Synthesizing {rows:,} transaction rows ...")
    txns = _synthesize(spark, rows)

    # Baseline A: one big CSV, unpartitioned, unsorted, all columns, no columnar
    # stats. coalesce(1) forces a single large file: the classic slow lake
    # shape a raw dump lands in before any optimization.
    print("Writing baseline A (single unpartitioned CSV, all columns) ...")
    if BASELINE_CSV.exists():
        shutil.rmtree(BASELINE_CSV)
    txns.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(BASELINE_CSV))

    # Baseline B: unoptimized Parquet. Same columnar file format as the
    # optimized side, but with none of the query-optimization work: not
    # partitioned, not sorted, all columns present, written as a few big files.
    # Holding the file format constant against the optimized layout isolates the
    # gain attributable purely to partitioning, column pruning, compaction,
    # pushdown, and SQL rewrites. Row-group stats exist because Parquet always
    # writes them, but without sorting they carry wide min/max ranges that
    # cannot skip much, and without partitioning there is no directory pruning.
    print("Writing baseline B (unpartitioned unsorted Parquet, all columns) ...")
    if BASELINE_PARQUET.exists():
        shutil.rmtree(BASELINE_PARQUET)
    txns.coalesce(4).write.mode("overwrite").parquet(str(BASELINE_PARQUET))

    # Optimized transactions: partitioned by month (a sensible grain, not
    # over-partitioned to the day), one compacted file per partition, only the
    # needed columns, sorted within partition on txn_date then the common
    # filter columns so Parquet row-group min/max stats enable predicate
    # pushdown and page skipping even for day-level and status filters.
    print("Writing optimized transactions (month-partitioned, compacted, pruned, sorted) ...")
    if OPTIMIZED_TXNS.exists():
        shutil.rmtree(OPTIMIZED_TXNS)
    needed = txns.select(
        "txn_id", "booking_id", "txn_date", "txn_month", "amount", "status", "currency", "payment_method"
    )
    num_months = NUM_DAYS // 30 + 2
    (
        needed.repartition(num_months, "txn_month")  # one file per month partition -> compaction
        .sortWithinPartitions("txn_date", "status", "amount")  # sort for min/max pushdown
        .write.mode("overwrite")
        .partitionBy("txn_month")
        .parquet(str(OPTIMIZED_TXNS))
    )

    # A small bookings dimension for the join query, keyed on booking_id.
    print("Writing optimized bookings dimension ...")
    if OPTIMIZED_BOOKINGS.exists():
        shutil.rmtree(OPTIMIZED_BOOKINGS)
    num_bookings = rows // 3 + 1
    bookings = (
        spark.range(0, num_bookings)
        .withColumnRenamed("id", "booking_id")
        .withColumn("listing_id", (F.col("booking_id") % F.lit(50000)))
        .withColumn("nights", (F.col("booking_id") % F.lit(14) + 1).cast("int"))
        .withColumn("guest_id", (F.col("booking_id") % F.lit(900000) + 100000))
    )
    bookings.repartition(8).write.mode("overwrite").parquet(str(OPTIMIZED_BOOKINGS))
    print("Datasets built.")


# ---------------------------------------------------------------------------
# Queries. Each returns a DataFrame; the harness triggers a full action.
# ---------------------------------------------------------------------------


def _target_month_bounds() -> tuple[str, str]:
    """A representative one-month date range inside the data window."""
    start = START_DATE + timedelta(days=400)
    end = start + timedelta(days=30)
    return start.isoformat(), end.isoformat()


def q1_date_range_agg(df: DataFrame) -> DataFrame:
    """Aggregate amount over a one-month window (partition pruning + stats)."""
    lo, hi = _target_month_bounds()
    return (
        df.where((F.col("txn_date") >= F.lit(lo)) & (F.col("txn_date") < F.lit(hi)))
        .groupBy("status")
        .agg(F.sum("amount").alias("total"), F.count("*").alias("n"))
    )


def q2_status_filter_group(df: DataFrame) -> DataFrame:
    """Filter to succeeded and group by method (column pruning + pushdown)."""
    return (
        df.where(F.col("status") == F.lit("succeeded"))
        .groupBy("payment_method")
        .agg(F.sum("amount").alias("total"), F.avg("amount").alias("avg_amt"))
    )


def q3_single_day_lookup(df: DataFrame) -> DataFrame:
    """Aggregate a single day (best case for partition pruning)."""
    day = (START_DATE + timedelta(days=500)).isoformat()
    return df.where(F.col("txn_date") == F.lit(day)).groupBy("currency").agg(F.sum("amount").alias("total"))


def q4_join_filter_before_join(df: DataFrame, bookings: DataFrame) -> DataFrame:
    """Join succeeded txns in a date window to bookings.

    The SQL rewrite: filter both sides down before the join and select only the
    join keys plus needed measures, rather than joining wide tables and
    filtering after (and never SELECT *).
    """
    lo, hi = _target_month_bounds()
    txn_slim = (
        df.where(
            (F.col("status") == F.lit("succeeded"))
            & (F.col("txn_date") >= F.lit(lo))
            & (F.col("txn_date") < F.lit(hi))
        )
        .select("booking_id", "amount")
    )
    book_slim = bookings.select("booking_id", "listing_id", "nights")
    return (
        txn_slim.join(book_slim, on="booking_id", how="inner")
        .groupBy("listing_id")
        .agg(F.sum("amount").alias("revenue"), F.sum("nights").alias("nights"))
    )


def q5_wide_scan_agg(df: DataFrame) -> DataFrame:
    """Global aggregation touching only two columns of a wide table.

    On the baseline this must read the full-width rows; on the optimized layout
    only two columns are read, isolating the column-pruning benefit.
    """
    return df.groupBy("status").agg(F.sum("amount").alias("total"), F.count("*").alias("n"))


QUERIES = [
    ("q1_date_range_agg", "Date-range aggregation (partition pruning + stats)"),
    ("q2_status_filter_group", "Status filter + group by (column pruning + pushdown)"),
    ("q3_single_day_lookup", "Single-day point lookup (partition pruning)"),
    ("q4_join_filter_before_join", "Join with filter-before-join rewrite"),
    ("q5_wide_scan_agg", "Wide-table scan aggregation (column pruning)"),
]


def _time_query(fn, runs: int) -> float:
    """Run fn once to warm up, then time `runs` executions, return the median.

    fn must build a DataFrame and trigger a full action (count) so the whole
    query executes. Wall-clock is used because that is what a user waits on.
    """
    # Warm-up (discarded): pays JIT, metadata, and filesystem cache costs once.
    fn()
    samples: list[float] = []
    for _ in range(runs):
        start = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - start)
    return statistics.median(samples)


def _query_fn(name: str, df: DataFrame, bookings: DataFrame):
    """Bind a query name and its input DataFrame to a no-arg timing callable.

    The callable triggers a full action (count) so the entire query executes.
    """
    builders = {
        "q1_date_range_agg": lambda: q1_date_range_agg(df),
        "q2_status_filter_group": lambda: q2_status_filter_group(df),
        "q3_single_day_lookup": lambda: q3_single_day_lookup(df),
        "q4_join_filter_before_join": lambda: q4_join_filter_before_join(df, bookings),
        "q5_wide_scan_agg": lambda: q5_wide_scan_agg(df),
    }
    build = builders[name]
    return lambda: build().count()


def _reduction(before: float, after: float) -> float:
    """Percentage runtime reduction going from `before` to `after`."""
    return (before - after) / before * 100.0 if before > 0 else 0.0


def run_benchmark(spark: SparkSession, runs: int) -> dict:
    """Time every query on all three layouts and return two comparisons.

    Layouts, all read with the same Spark session and config so the only
    variable is the physical layout:
      - baseline A: raw unoptimized CSV (row format, no partitioning/stats)
      - baseline B: unoptimized Parquet (columnar, but no partitioning, no
        sorting, all columns) -- the fair "before" for query optimization
      - optimized: month-partitioned, compacted, column-pruned, sorted Parquet

    Returns per-query medians for all three layouts plus two headline
    reductions: format+layout (CSV -> optimized) and query-optimization-only
    (unoptimized Parquet -> optimized).
    """
    csv_baseline = spark.read.option("header", "true").option("inferSchema", "true").csv(str(BASELINE_CSV))
    parquet_baseline = spark.read.parquet(str(BASELINE_PARQUET))
    optimized = spark.read.parquet(str(OPTIMIZED_TXNS))
    bookings = spark.read.parquet(str(OPTIMIZED_BOOKINGS))

    per_query = []
    total_csv = 0.0
    total_parquet = 0.0
    total_optimized = 0.0
    for name, desc in QUERIES:
        print(f"Timing {name} on raw CSV baseline ...")
        csv_med = _time_query(_query_fn(name, csv_baseline, bookings), runs)
        print(f"Timing {name} on unoptimized Parquet baseline ...")
        parquet_med = _time_query(_query_fn(name, parquet_baseline, bookings), runs)
        print(f"Timing {name} on optimized layout ...")
        opt_med = _time_query(_query_fn(name, optimized, bookings), runs)

        total_csv += csv_med
        total_parquet += parquet_med
        total_optimized += opt_med
        per_query.append(
            {
                "query": name,
                "description": desc,
                "csv_baseline_median_s": round(csv_med, 4),
                "parquet_baseline_median_s": round(parquet_med, 4),
                "optimized_median_s": round(opt_med, 4),
                "format_layout_reduction_pct": round(_reduction(csv_med, opt_med), 2),
                "query_opt_reduction_pct": round(_reduction(parquet_med, opt_med), 2),
                "query_opt_speedup_x": round(parquet_med / opt_med, 2) if opt_med > 0 else None,
            }
        )
        print(
            f"  {name}: csv {csv_med:.3f}s  parquet {parquet_med:.3f}s  optimized {opt_med:.3f}s  "
            f"(query-opt reduction {_reduction(parquet_med, opt_med):.1f}%)"
        )

    return {
        "runs_per_query": runs,
        "query_count": len(QUERIES),
        "total_csv_baseline_median_s": round(total_csv, 4),
        "total_parquet_baseline_median_s": round(total_parquet, 4),
        "total_optimized_median_s": round(total_optimized, 4),
        "format_layout_reduction_pct": round(_reduction(total_csv, total_optimized), 2),
        "format_layout_speedup_x": round(total_csv / total_optimized, 2) if total_optimized > 0 else None,
        "query_opt_reduction_pct": round(_reduction(total_parquet, total_optimized), 2),
        "query_opt_speedup_x": round(total_parquet / total_optimized, 2) if total_optimized > 0 else None,
        "per_query": per_query,
    }


def _folder_size_bytes(path: Path) -> int:
    """Total size in bytes of all files under a directory."""
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def write_results(results: dict, rows: int) -> None:
    """Write results.json and a two-comparison markdown report."""
    results["dataset_rows"] = rows
    results["csv_baseline_bytes"] = _folder_size_bytes(BASELINE_CSV) if BASELINE_CSV.exists() else None
    results["parquet_baseline_bytes"] = _folder_size_bytes(BASELINE_PARQUET) if BASELINE_PARQUET.exists() else None
    results["optimized_bytes"] = _folder_size_bytes(OPTIMIZED_TXNS) if OPTIMIZED_TXNS.exists() else None

    (BENCH_ROOT / "results.json").write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")

    lines = [
        "# Query Optimization Benchmark Results",
        "",
        f"- Dataset: {rows:,} synthetic transaction rows spanning {NUM_DAYS} days",
        f"- Queries: {results['query_count']} representative analytical queries",
        f"- Timing: median of {results['runs_per_query']} timed wall-clock runs per query after one warm-up",
        "- Three layouts on the same data and queries, same Spark config:",
        "  - Baseline A: raw unoptimized CSV (row format, unpartitioned, all columns, no stats)",
        "  - Baseline B: unoptimized Parquet (columnar, but unpartitioned, unsorted, all columns)",
        "  - Optimized: month-partitioned, compacted, column-pruned, sorted Parquet with row-group stats",
        "",
        "## Two measured reductions",
        "",
        f"- **Format + layout (CSV to optimized Parquet): {results['format_layout_reduction_pct']}%** "
        f"({results['format_layout_speedup_x']}x). Includes the row-to-columnar file-format change.",
        f"- **Query optimization only (unoptimized Parquet to optimized Parquet): "
        f"{results['query_opt_reduction_pct']}%** ({results['query_opt_speedup_x']}x). File format held "
        f"constant, so this is attributable purely to partitioning, column pruning, file compaction, "
        f"predicate pushdown, and SQL rewrites.",
        "",
        f"Totals across all queries: CSV {results['total_csv_baseline_median_s']}s, unoptimized Parquet "
        f"{results['total_parquet_baseline_median_s']}s, optimized {results['total_optimized_median_s']}s.",
        "",
        "## Query optimization only: unoptimized Parquet vs optimized Parquet",
        "",
        "This is the honest number for the resume bullet's listed techniques.",
        "",
        "| Query | Technique | Unopt. Parquet (s) | Optimized (s) | Speedup | Reduction |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for row in results["per_query"]:
        lines.append(
            f"| {row['query']} | {row['description']} | {row['parquet_baseline_median_s']} | "
            f"{row['optimized_median_s']} | {row['query_opt_speedup_x']}x | {row['query_opt_reduction_pct']}% |"
        )
    lines += [
        "",
        "## Format + layout: raw CSV vs optimized Parquet",
        "",
        "| Query | Technique | Raw CSV (s) | Optimized (s) | Reduction |",
        "| --- | --- | ---: | ---: | ---: |",
    ]
    for row in results["per_query"]:
        lines.append(
            f"| {row['query']} | {row['description']} | {row['csv_baseline_median_s']} | "
            f"{row['optimized_median_s']} | {row['format_layout_reduction_pct']}% |"
        )
    lines.append("")
    (BENCH_ROOT / "RESULTS.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\nWrote {BENCH_ROOT / 'results.json'} and {BENCH_ROOT / 'RESULTS.md'}")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query-optimization benchmark: baseline vs optimized lake.")
    parser.add_argument("--rows", type=int, default=20_000_000, help="Synthetic transaction rows.")
    parser.add_argument("--runs", type=int, default=3, help="Timed runs per query (median is reported).")
    parser.add_argument("--rebuild", action="store_true", help="Rebuild datasets even if present.")
    return parser.parse_args(argv)


def main(argv: list[str]) -> None:
    """Build datasets if needed, run the benchmark, and write results."""
    args = _parse_args(argv)
    DATA_ROOT.mkdir(parents=True, exist_ok=True)
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        need_build = (
            args.rebuild
            or not OPTIMIZED_TXNS.exists()
            or not BASELINE_CSV.exists()
            or not BASELINE_PARQUET.exists()
        )
        if need_build:
            build_datasets(spark, args.rows)
        else:
            print("Reusing existing datasets under benchmark/_data (pass --rebuild to regenerate).")
        results = run_benchmark(spark, args.runs)
        write_results(results, args.rows)
        print(f"\nFORMAT + LAYOUT REDUCTION (CSV -> optimized): {results['format_layout_reduction_pct']}%")
        print(f"QUERY-OPTIMIZATION-ONLY REDUCTION (Parquet -> optimized): {results['query_opt_reduction_pct']}%")
    finally:
        spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
