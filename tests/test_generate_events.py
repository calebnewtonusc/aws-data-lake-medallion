"""Tests for the synthetic Airbnb data generator."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from generate_events import generate  # noqa: E402


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
