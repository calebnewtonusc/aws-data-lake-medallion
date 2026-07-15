# Analytics-Ready Gold Data Models

The gold layer turns the cleaned silver tables into analytics-ready data models:
well-defined, documented tables shaped for the questions analysts actually ask,
rather than raw normalized facts. The design is a star-schema-style layout: a
small set of conformed dimensions surrounded by fact and pre-aggregated summary
tables, all keyed so BI tools and Athena can join them without surprises.

Everything here is partitioned Parquet in S3 (or local disk offline). The
benchmark's optimized layout and the `scale_generate_emr.py` job both write these
grains, so the models proven locally are the same ones that scale to 250+ GB on
AWS.

## The four source domains

| Domain       | Grain               | Role in the model         |
| ------------ | ------------------- | ------------------------- |
| listings     | one row per listing | conformed dimension       |
| reviews      | one row per review  | fact (review events)      |
| bookings     | one row per booking | fact (reservation events) |
| transactions | one row per payment | fact (payment events)     |

Bookings reference listings, transactions reference bookings and listings, and
reviews reference listings, so all four join cleanly on their natural keys.

## Dimension

### dim_listing

The conformed listing dimension every fact joins to.

| Column         | Type   | Notes              |
| -------------- | ------ | ------------------ |
| listing_id     | bigint | primary key        |
| neighbourhood  | string | grouping attribute |
| room_type      | string | grouping attribute |
| price          | double | nightly price      |
| minimum_nights | int    | booking constraint |

Stored unpartitioned and compacted to a single sorted file: it is small and read
in full by nearly every join, so partitioning would only add overhead.

## Fact tables (partitioned, sorted for pushdown)

### fact_transactions

Payment events, partitioned by `txn_month`, sorted within partition by
`txn_date, status, amount`.

| Column         | Type   | Notes                              |
| -------------- | ------ | ---------------------------------- |
| txn_id         | bigint | primary key                        |
| booking_id     | bigint | foreign key to fact_bookings       |
| listing_id     | bigint | foreign key to dim_listing         |
| txn_date       | date   | sort key, drives partition pruning |
| txn_month      | string | **partition key**                  |
| amount         | double | charge amount                      |
| status         | string | succeeded / failed / refunded      |
| currency       | string | USD / EUR / GBP / AUD              |
| payment_method | string | card / paypal / apple_pay / ...    |

### fact_bookings

Reservation events, partitioned by `checkin_month`, sorted by
`checkin_date, status, amount`.

| Column        | Type   | Notes                                       |
| ------------- | ------ | ------------------------------------------- |
| booking_id    | bigint | primary key                                 |
| listing_id    | bigint | foreign key to dim_listing                  |
| guest_id      | bigint | guest reference                             |
| checkin_date  | date   | sort key, drives partition pruning          |
| checkin_month | string | **partition key**                           |
| nights        | int    | stay length                                 |
| amount        | double | booking total                               |
| status        | string | completed / confirmed / cancelled / no_show |

### fact_reviews

Review events, partitioned by `review_month`, sorted by `review_date, rating`.

| Column       | Type   | Notes                              |
| ------------ | ------ | ---------------------------------- |
| review_id    | bigint | primary key                        |
| listing_id   | bigint | foreign key to dim_listing         |
| reviewer_id  | bigint | reviewer reference                 |
| review_date  | date   | sort key, drives partition pruning |
| review_month | string | **partition key**                  |
| rating       | int    | 1 to 5                             |

## Pre-aggregated summary table (optimized table design)

### gold_revenue_by_listing_month

A denormalized, pre-aggregated star summary: succeeded revenue rolled up to one
row per listing per month, already joined to the listing dimension so the common
revenue questions need no fact scan or dimension join at query time. Partitioned
by `txn_month`, sorted by `listing_id`.

| Column        | Type   | Notes                                |
| ------------- | ------ | ------------------------------------ |
| listing_id    | bigint | grain key                            |
| txn_month     | string | **partition key**, grain key         |
| revenue       | double | sum of succeeded transaction amount  |
| txn_count     | bigint | number of succeeded transactions     |
| avg_ticket    | double | average succeeded transaction amount |
| neighbourhood | string | denormalized from dim_listing        |
| room_type     | string | denormalized from dim_listing        |

This table is the "optimized table design" technique in the query-optimization
benchmark: the running-total, top-N-per-month, moving-average, month-over-month,
and revenue-by-neighbourhood workloads read it directly instead of re-joining and
re-aggregating the raw transactions fact. Because the revenue answer is already
materialized, denormalized, partitioned, and sorted, those queries do a fraction
of the work of the normalized baseline.

## Why this shape

- **Conformed dimension**: `dim_listing` is joined the same way by every fact, so
  neighbourhood and room-type rollups are consistent across revenue, bookings,
  and reviews.
- **Partitioned facts**: month partitions plus in-partition sort keys give both
  directory-level partition pruning and row-group-level predicate pushdown for the
  date-bounded queries that dominate analytics.
- **A summary table for the hot path**: revenue by listing and month is the most
  asked question, so it is pre-aggregated and denormalized once at build time
  rather than recomputed on every query.

The measured impact of this design is in `benchmark/RESULTS.md`.
