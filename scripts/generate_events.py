"""Synthetic Airbnb listings and reviews generator.

Produces two related datasets that mirror the Inside Airbnb open data used in
the ZTM Data Engineering course: a listings table and a reviews table joined
on listing id. A configurable fraction of review records are intentionally
dirty (missing listing id, blank comments, malformed dates, duplicates) so
the silver cleaning and data-quality steps have something real to catch.

Run standalone to write newline-delimited JSON for each dataset:

    python scripts/generate_events.py --listings-out listings.jsonl --reviews-out reviews.jsonl
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from datetime import date, timedelta
from typing import Any

NEIGHBOURHOODS: list[str] = [
    "Downtown",
    "Mission District",
    "Capitol Hill",
    "Williamsburg",
    "Shoreditch",
    "Le Marais",
    "Kreuzberg",
    "Fitzroy",
]

ROOM_TYPES: list[str] = ["Entire home/apt", "Private room", "Shared room", "Hotel room"]

LISTING_ADJECTIVES: list[str] = ["Cozy", "Bright", "Modern", "Charming", "Spacious", "Quiet", "Central", "Sunny"]
LISTING_NOUNS: list[str] = ["Loft", "Studio", "Apartment", "Cottage", "Flat", "Suite", "Bungalow", "Townhouse"]

REVIEWER_NAMES: list[str] = [
    "Alex",
    "Jordan",
    "Priya",
    "Marco",
    "Yuki",
    "Fatima",
    "Liam",
    "Sofia",
    "Noah",
    "Elena",
]

COMMENT_TEMPLATES: list[str] = [
    "Great stay, the host was very responsive and the place was spotless.",
    "Excellent location, walkable to everything. Would book again.",
    "Comfortable and quiet, exactly as described in the listing.",
    "Loved the neighborhood. The apartment had everything we needed.",
    "Smooth check-in and a beautiful space. Highly recommend.",
    "A little noisy at night but overall a pleasant experience.",
    "The host went above and beyond to make us feel welcome.",
]


def _random_date(start: date, end: date) -> date:
    """Return a random date within the inclusive range."""
    span = (end - start).days
    return start + timedelta(days=random.randint(0, span))


def make_listing(listing_id: int) -> dict[str, Any]:
    """Build one Airbnb listing record.

    Args:
        listing_id: Stable integer id used to join reviews.

    Returns:
        A JSON-serializable listing dictionary.
    """
    name = f"{random.choice(LISTING_ADJECTIVES)} {random.choice(LISTING_NOUNS)} in {random.choice(NEIGHBOURHOODS)}"
    return {
        "id": listing_id,
        "name": name,
        "host_id": random.randint(100_000, 999_999),
        "neighbourhood": random.choice(NEIGHBOURHOODS),
        "room_type": random.choice(ROOM_TYPES),
        "price": round(random.uniform(45.0, 650.0), 2),
        "minimum_nights": random.choice([1, 1, 2, 2, 3, 5, 7]),
    }


def make_review(review_id: int, listing_id: int, dirty: bool) -> dict[str, Any]:
    """Build one Airbnb review record.

    Args:
        review_id: Unique review id.
        listing_id: The listing this review belongs to.
        dirty: When true, inject a realistic data-quality defect.

    Returns:
        A JSON-serializable review dictionary.
    """
    review: dict[str, Any] = {
        "id": review_id,
        "listing_id": listing_id,
        "date": _random_date(date(2024, 1, 1), date(2024, 11, 30)).isoformat(),
        "reviewer_id": random.randint(1_000_000, 9_999_999),
        "reviewer_name": random.choice(REVIEWER_NAMES),
        "rating": random.randint(1, 5),
        "comments": random.choice(COMMENT_TEMPLATES),
    }

    if not dirty:
        return review

    defect = random.choice(["missing_listing", "blank_comment", "bad_date", "null_reviewer", "bad_rating"])
    if defect == "missing_listing":
        review["listing_id"] = None
    elif defect == "blank_comment":
        review["comments"] = "   "
    elif defect == "bad_date":
        review["date"] = "not-a-date"
    elif defect == "null_reviewer":
        review["reviewer_id"] = None
    elif defect == "bad_rating":
        review["rating"] = 0
    return review


def generate(
    num_listings: int,
    reviews_per_listing: int,
    dirty_ratio: float,
    duplicate_ratio: float,
    seed: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Generate related listings and reviews with defects and duplicates.

    Args:
        num_listings: Number of listings to create.
        reviews_per_listing: Average reviews generated per listing.
        dirty_ratio: Fraction of reviews carrying a data-quality defect.
        duplicate_ratio: Fraction of clean reviews re-emitted as duplicates.
        seed: Random seed for reproducible output.

    Returns:
        A tuple of (listings, reviews).
    """
    random.seed(seed)
    listings = [make_listing(1000 + i) for i in range(num_listings)]

    reviews: list[dict[str, Any]] = []
    review_id = 5_000_000
    for listing in listings:
        for _ in range(random.randint(1, reviews_per_listing * 2)):
            review_id += 1
            dirty = random.random() < dirty_ratio
            review = make_review(review_id, listing["id"], dirty)
            reviews.append(review)
            if not dirty and random.random() < duplicate_ratio:
                reviews.append(dict(review))

    random.shuffle(reviews)
    return listings, reviews


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic Airbnb listings and reviews.")
    parser.add_argument("--num-listings", type=int, default=400, help="Number of listings.")
    parser.add_argument("--reviews-per-listing", type=int, default=6, help="Average reviews per listing.")
    parser.add_argument("--dirty-ratio", type=float, default=0.08, help="Fraction of defective reviews.")
    parser.add_argument("--duplicate-ratio", type=float, default=0.05, help="Fraction of clean reviews duplicated.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    parser.add_argument("--listings-out", type=str, default="-", help="Listings output file, or - for stdout.")
    parser.add_argument("--reviews-out", type=str, default="-", help="Reviews output file, or - for stdout.")
    return parser.parse_args(argv)


def _write(records: list[dict[str, Any]], path: str) -> None:
    """Write records as newline-delimited JSON to a file or stdout."""
    lines = "\n".join(json.dumps(r) for r in records)
    if path == "-":
        sys.stdout.write(lines + "\n")
    else:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(lines + "\n")


def main(argv: list[str]) -> None:
    """Command-line entry point writing listings and reviews as JSON lines."""
    args = _parse_args(argv)
    listings, reviews = generate(
        args.num_listings, args.reviews_per_listing, args.dirty_ratio, args.duplicate_ratio, args.seed
    )
    _write(listings, args.listings_out)
    _write(reviews, args.reviews_out)


if __name__ == "__main__":
    main(sys.argv[1:])
