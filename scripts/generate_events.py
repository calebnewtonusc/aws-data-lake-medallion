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

# Booking lifecycle statuses. Weighted toward completed so the marketplace
# looks healthy while still leaving a realistic tail of cancellations.
BOOKING_STATUSES: list[str] = [
    "completed",
    "completed",
    "completed",
    "completed",
    "confirmed",
    "confirmed",
    "cancelled",
    "no_show",
]

# Transaction outcomes. Most payments succeed; a minority fail or are refunded
# so the gold transaction-success-rate table has signal to report.
TXN_STATUSES: list[str] = ["succeeded", "succeeded", "succeeded", "succeeded", "succeeded", "failed", "refunded"]

PAYMENT_METHODS: list[str] = ["card", "card", "card", "paypal", "apple_pay", "google_pay", "bank_transfer"]

CURRENCIES: list[str] = ["USD", "USD", "USD", "EUR", "GBP", "AUD"]


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


def make_booking(booking_id: int, listing_id: int, dirty: bool) -> dict[str, Any]:
    """Build one booking record (a reserved stay against a listing).

    Args:
        booking_id: Unique booking id.
        listing_id: The listing being booked.
        dirty: When true, inject a realistic data-quality defect.

    Returns:
        A JSON-serializable booking dictionary. Fields: booking_id, listing_id,
        guest_id, checkin_date, checkout_date, nights, amount, status.
    """
    checkin = _random_date(date(2024, 1, 1), date(2024, 12, 15))
    nights = random.choice([1, 2, 2, 3, 3, 4, 5, 7, 10, 14])
    checkout = checkin + timedelta(days=nights)
    nightly = round(random.uniform(45.0, 650.0), 2)
    booking: dict[str, Any] = {
        "booking_id": booking_id,
        "listing_id": listing_id,
        "guest_id": random.randint(2_000_000, 8_999_999),
        "checkin_date": checkin.isoformat(),
        "checkout_date": checkout.isoformat(),
        "nights": nights,
        "amount": round(nightly * nights, 2),
        "status": random.choice(BOOKING_STATUSES),
    }

    if not dirty:
        return booking

    defect = random.choice(["missing_listing", "bad_checkin", "null_guest", "bad_nights", "bad_amount"])
    if defect == "missing_listing":
        booking["listing_id"] = None
    elif defect == "bad_checkin":
        booking["checkin_date"] = "not-a-date"
    elif defect == "null_guest":
        booking["guest_id"] = None
    elif defect == "bad_nights":
        booking["nights"] = 0
    elif defect == "bad_amount":
        booking["amount"] = -1.0
    return booking


def make_transaction(txn_id: int, booking_id: int, booking_amount: float, dirty: bool) -> dict[str, Any]:
    """Build one transaction record (a payment event for a booking).

    Args:
        txn_id: Unique transaction id.
        booking_id: The booking this payment settles.
        booking_amount: The booking total, used as the charge base.
        dirty: When true, inject a realistic data-quality defect.

    Returns:
        A JSON-serializable transaction dictionary. Fields: txn_id, booking_id,
        ts, amount, currency, payment_method, status.
    """
    ts = _random_date(date(2024, 1, 1), date(2024, 12, 20))
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    txn: dict[str, Any] = {
        "txn_id": txn_id,
        "booking_id": booking_id,
        "ts": f"{ts.isoformat()}T{hour:02d}:{minute:02d}:{second:02d}",
        "amount": booking_amount,
        "currency": random.choice(CURRENCIES),
        "payment_method": random.choice(PAYMENT_METHODS),
        "status": random.choice(TXN_STATUSES),
    }

    if not dirty:
        return txn

    defect = random.choice(["missing_booking", "bad_ts", "null_amount", "blank_currency", "blank_method"])
    if defect == "missing_booking":
        txn["booking_id"] = None
    elif defect == "bad_ts":
        txn["ts"] = "not-a-timestamp"
    elif defect == "null_amount":
        txn["amount"] = None
    elif defect == "blank_currency":
        txn["currency"] = "  "
    elif defect == "blank_method":
        txn["payment_method"] = ""
    return txn


def generate_all(
    num_listings: int,
    reviews_per_listing: int,
    bookings_per_listing: int,
    dirty_ratio: float,
    duplicate_ratio: float,
    seed: int,
) -> dict[str, list[dict[str, Any]]]:
    """Generate all four related datasets with defects and duplicates.

    Listings and reviews match the original two-dataset generator. Bookings are
    created per listing, and each successful-looking booking spawns one or more
    transactions referencing it, so the four tables join cleanly on their keys.

    Args:
        num_listings: Number of listings to create.
        reviews_per_listing: Average reviews generated per listing.
        bookings_per_listing: Average bookings generated per listing.
        dirty_ratio: Fraction of records carrying a data-quality defect.
        duplicate_ratio: Fraction of clean records re-emitted as duplicates.
        seed: Random seed for reproducible output.

    Returns:
        A dict with keys listings, reviews, bookings, transactions.
    """
    listings, reviews = generate(num_listings, reviews_per_listing, dirty_ratio, duplicate_ratio, seed)

    # Continue the same seeded stream so bookings and transactions are
    # deterministic alongside the listings and reviews above.
    bookings: list[dict[str, Any]] = []
    transactions: list[dict[str, Any]] = []
    booking_id = 7_000_000
    txn_id = 9_000_000
    for listing in listings:
        for _ in range(random.randint(1, bookings_per_listing * 2)):
            booking_id += 1
            dirty_booking = random.random() < dirty_ratio
            booking = make_booking(booking_id, listing["id"], dirty_booking)
            bookings.append(booking)
            if not dirty_booking and random.random() < duplicate_ratio:
                bookings.append(dict(booking))

            # Payment events only for bookings that were not cancelled outright.
            if booking.get("status") in ("completed", "confirmed"):
                for _ in range(random.choice([1, 1, 1, 2])):
                    txn_id += 1
                    dirty_txn = random.random() < dirty_ratio
                    base_amount = booking.get("amount") if isinstance(booking.get("amount"), (int, float)) else 0.0
                    txn = make_transaction(txn_id, booking_id, float(base_amount or 0.0), dirty_txn)
                    transactions.append(txn)
                    if not dirty_txn and random.random() < duplicate_ratio:
                        transactions.append(dict(txn))

    random.shuffle(bookings)
    random.shuffle(transactions)
    return {
        "listings": listings,
        "reviews": reviews,
        "bookings": bookings,
        "transactions": transactions,
    }


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic Airbnb listings and reviews.")
    parser.add_argument("--num-listings", type=int, default=400, help="Number of listings.")
    parser.add_argument("--reviews-per-listing", type=int, default=6, help="Average reviews per listing.")
    parser.add_argument("--bookings-per-listing", type=int, default=4, help="Average bookings per listing.")
    parser.add_argument("--dirty-ratio", type=float, default=0.08, help="Fraction of defective records.")
    parser.add_argument("--duplicate-ratio", type=float, default=0.05, help="Fraction of clean records duplicated.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    parser.add_argument("--listings-out", type=str, default="-", help="Listings output file, or - for stdout.")
    parser.add_argument("--reviews-out", type=str, default="-", help="Reviews output file, or - for stdout.")
    parser.add_argument("--bookings-out", type=str, default="", help="Bookings output file. Empty to skip.")
    parser.add_argument("--transactions-out", type=str, default="", help="Transactions output file. Empty to skip.")
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
    """Command-line entry point writing the datasets as JSON lines.

    Listings and reviews always write. Bookings and transactions write only
    when their output paths are supplied, so the original two-dataset behaviour
    is preserved by default.
    """
    args = _parse_args(argv)
    datasets = generate_all(
        args.num_listings,
        args.reviews_per_listing,
        args.bookings_per_listing,
        args.dirty_ratio,
        args.duplicate_ratio,
        args.seed,
    )
    _write(datasets["listings"], args.listings_out)
    _write(datasets["reviews"], args.reviews_out)
    if args.bookings_out:
        _write(datasets["bookings"], args.bookings_out)
    if args.transactions_out:
        _write(datasets["transactions"], args.transactions_out)


if __name__ == "__main__":
    main(sys.argv[1:])
