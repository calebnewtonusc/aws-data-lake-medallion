"""Tests for the synthetic Airbnb data generator."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from generate_events import generate, generate_all  # noqa: E402


def test_generate_is_deterministic() -> None:
    """The same seed produces identical listings and reviews."""
    a_listings, a_reviews = generate(50, 5, 0.1, 0.05, seed=7)
    b_listings, b_reviews = generate(50, 5, 0.1, 0.05, seed=7)
    assert a_listings == b_listings
    assert a_reviews == b_reviews


def test_generate_listing_count() -> None:
    """The number of listings matches the requested count."""
    listings, _ = generate(30, 4, 0.0, 0.0, seed=1)
    assert len(listings) == 30


def test_generate_injects_defects() -> None:
    """A non-zero dirty ratio produces at least one defective review."""
    _, reviews = generate(100, 6, 0.5, 0.0, seed=3)
    defective = [
        r
        for r in reviews
        if r.get("listing_id") is None
        or r.get("reviewer_id") is None
        or r.get("comments", "").strip() == ""
        or r.get("date") == "not-a-date"
        or r.get("rating") == 0
    ]
    assert defective, "Expected injected data-quality defects"


def test_reviews_reference_real_listings() -> None:
    """Every clean review references a generated listing id."""
    listings, reviews = generate(40, 5, 0.0, 0.0, seed=9)
    listing_ids = {listing["id"] for listing in listings}
    assert all(r["listing_id"] in listing_ids for r in reviews)


def test_generate_all_returns_four_datasets() -> None:
    """generate_all produces all four related datasets, all non-empty."""
    data = generate_all(30, 5, 4, 0.0, 0.0, seed=11)
    assert set(data) == {"listings", "reviews", "bookings", "transactions"}
    assert len(data["listings"]) == 30
    for name in ("reviews", "bookings", "transactions"):
        assert data[name], f"expected non-empty {name}"


def test_generate_all_is_deterministic() -> None:
    """The same seed produces identical four-dataset output."""
    a = generate_all(25, 4, 3, 0.1, 0.05, seed=5)
    b = generate_all(25, 4, 3, 0.1, 0.05, seed=5)
    assert a == b


def test_bookings_have_required_fields() -> None:
    """Every clean booking carries the full booking schema and references a listing."""
    data = generate_all(20, 3, 3, 0.0, 0.0, seed=2)
    listing_ids = {listing["id"] for listing in data["listings"]}
    required = {"booking_id", "listing_id", "guest_id", "checkin_date", "checkout_date", "nights", "amount", "status"}
    for booking in data["bookings"]:
        assert required.issubset(booking), "booking missing required fields"
        assert booking["listing_id"] in listing_ids


def test_transactions_reference_real_bookings() -> None:
    """Every transaction references a generated booking id and has all fields."""
    data = generate_all(20, 3, 4, 0.0, 0.0, seed=8)
    booking_ids = {b["booking_id"] for b in data["bookings"]}
    required = {"txn_id", "booking_id", "ts", "amount", "currency", "payment_method", "status"}
    for txn in data["transactions"]:
        assert required.issubset(txn), "transaction missing required fields"
        assert txn["booking_id"] in booking_ids
