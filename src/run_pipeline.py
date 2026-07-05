"""End-to-end medallion pipeline entry point for the Airbnb reviews domain.

Running `python -m src.run_pipeline` spins up an in-process mock S3 with moto,
generates synthetic Airbnb listings and reviews, lands them in the bronze
layer, runs the silver cleaning job, gates the reviews with a data-quality
check, builds the gold aggregates, and prints row counts for every layer plus
a sample of gold output. No AWS account or network access is required.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

from moto import mock_aws

from . import bronze, config, gold, quality, storage
from .spark_session import build_spark

sys.path.insert(0, "scripts")
from generate_events import generate  # noqa: E402


def _print_header(title: str) -> None:
    """Print a consistent section header to stdout."""
    print(f"\n=== {title} ===")


def run(num_listings: int, ingest_date: str, seed: int) -> dict[str, object]:
    """Run bronze, silver, quality, and gold end to end.

    Args:
        num_listings: Number of listings to generate.
        ingest_date: ISO ingest date for the bronze partitions.
        seed: Random seed for reproducible generation.

    Returns:
        A dictionary of layer names to row counts and the quality report.
    """
    results: dict[str, object] = {}

    _print_header("Bronze: landing raw Airbnb data in mock S3")
    client = storage.s3_client()
    storage.ensure_bucket(client)
    listings, reviews = generate(
        num_listings=num_listings,
        reviews_per_listing=6,
        dirty_ratio=0.08,
        duplicate_ratio=0.05,
        seed=seed,
    )
    listings_landed = bronze.land_dataset(client, config.LISTINGS_DATASET, listings, ingest_date=ingest_date)
    reviews_landed = bronze.land_dataset(client, config.REVIEWS_DATASET, reviews, ingest_date=ingest_date)
    listings_verified = bronze.count_landed(client, config.LISTINGS_DATASET, ingest_date=ingest_date)
    reviews_verified = bronze.count_landed(client, config.REVIEWS_DATASET, ingest_date=ingest_date)
    results["bronze_listings"] = listings_landed
    results["bronze_reviews"] = reviews_landed
    print(f"Landed {listings_landed} listings and {reviews_landed} reviews to {config.BRONZE.s3_uri()}")
    print(f"Verified {listings_verified} listings and {reviews_verified} reviews readable back through the S3 API")

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        from . import silver as silver_job

        _print_header("Silver: cleaning, validating, deduplicating")
        silver_counts = silver_job.run(spark)
        results["silver_counts"] = silver_counts
        print(f"Wrote {silver_counts['listings']} clean listings and {silver_counts['reviews']} clean reviews to silver")

        _print_header("Data quality: gating the silver reviews table")
        silver_reviews = gold.read_silver_reviews(spark)
        report = quality.run_checks(silver_reviews)
        results["quality_report"] = report
        print(
            f"Passed: {report.row_count} reviews, {report.distinct_review_ids} unique review ids, "
            f"0 duplicates, 0 null keys, 0 out-of-range ratings"
        )

        _print_header("Gold: building business aggregates")
        gold_counts = gold.run(spark)
        results["gold_counts"] = gold_counts
        for name, rows in gold_counts.items():
            print(f"Wrote {rows} rows to {config.GOLD.s3_uri()}{name}/")

        silver_listings = gold.read_silver_listings(spark)
        _print_header("Sample gold output: reviews per listing (top 10)")
        gold.reviews_per_listing(silver_listings, silver_reviews).show(10, truncate=False)

        _print_header("Sample gold output: average rating per listing (top 10)")
        gold.avg_rating_per_listing(silver_listings, silver_reviews).show(10, truncate=False)

        _print_header("Sample gold output: reviews per neighbourhood")
        gold.reviews_per_neighbourhood(silver_listings, silver_reviews).show(20, truncate=False)
    finally:
        spark.stop()

    _print_header("Pipeline summary")
    print(f"bronze: {results['bronze_listings']} listings + {results['bronze_reviews']} reviews landed")
    silver_counts = results["silver_counts"]
    assert isinstance(silver_counts, dict)
    print(f"silver: {silver_counts['listings']} listings + {silver_counts['reviews']} reviews")
    gold_counts = results["gold_counts"]
    assert isinstance(gold_counts, dict)
    for name, rows in gold_counts.items():
        print(f"gold.{name}: {rows} rows")

    return results


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Airbnb medallion data lake pipeline locally.")
    parser.add_argument("--num-listings", type=int, default=400, help="Number of listings to generate.")
    parser.add_argument("--ingest-date", type=str, default=date.today().isoformat(), help="Bronze partition date.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    return parser.parse_args(argv)


@mock_aws
def main(argv: list[str]) -> None:
    """Command-line entry point wrapped in a moto mock S3 context."""
    args = _parse_args(argv)
    run(num_listings=args.num_listings, ingest_date=args.ingest_date, seed=args.seed)


if __name__ == "__main__":
    main(sys.argv[1:])
