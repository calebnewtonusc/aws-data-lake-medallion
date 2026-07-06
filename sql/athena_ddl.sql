-- Athena / Glue external table definitions for the Airbnb medallion data lake.
--
-- These statements register schema-on-read tables over the parquet the gold
-- and silver Spark jobs write to S3. Athena reads the files in place; nothing
-- is copied. Replace ztm-data-engineering-bootcamp with your bucket if you
-- deploy to your own account.
--
-- Run these in the Athena query editor after the pipeline has written to S3,
-- or register them through a Glue crawler.

CREATE DATABASE IF NOT EXISTS airbnb_lake;

-- Silver reviews fact table, partitioned by review month.
CREATE EXTERNAL TABLE IF NOT EXISTS airbnb_lake.silver_reviews (
    id            BIGINT,
    listing_id    BIGINT,
    reviewer_id   BIGINT,
    reviewer_name STRING,
    rating        INT,
    comments      STRING,
    review_date   DATE
)
PARTITIONED BY (review_month STRING)
STORED AS PARQUET
LOCATION 's3://ztm-data-engineering-bootcamp/silver/reviews/';

-- Silver listings dimension.
CREATE EXTERNAL TABLE IF NOT EXISTS airbnb_lake.silver_listings (
    id             BIGINT,
    name           STRING,
    host_id        BIGINT,
    neighbourhood  STRING,
    room_type      STRING,
    price          DOUBLE,
    minimum_nights INT
)
STORED AS PARQUET
LOCATION 's3://ztm-data-engineering-bootcamp/silver/listings/';

-- Gold: reviews per listing, the course's flagship aggregate.
CREATE EXTERNAL TABLE IF NOT EXISTS airbnb_lake.gold_reviews_per_listing (
    id            BIGINT,
    name          STRING,
    neighbourhood STRING,
    num_reviews   BIGINT
)
STORED AS PARQUET
LOCATION 's3://ztm-data-engineering-bootcamp/gold/reviews_per_listing/';

-- Gold: average rating per listing.
CREATE EXTERNAL TABLE IF NOT EXISTS airbnb_lake.gold_avg_rating_per_listing (
    id          BIGINT,
    name        STRING,
    avg_rating  DOUBLE,
    num_reviews BIGINT
)
STORED AS PARQUET
LOCATION 's3://ztm-data-engineering-bootcamp/gold/avg_rating_per_listing/';

-- Gold: reviews per neighbourhood.
CREATE EXTERNAL TABLE IF NOT EXISTS airbnb_lake.gold_reviews_per_neighbourhood (
    neighbourhood STRING,
    num_reviews   BIGINT,
    avg_rating    DOUBLE,
    num_listings  BIGINT
)
STORED AS PARQUET
LOCATION 's3://ztm-data-engineering-bootcamp/gold/reviews_per_neighbourhood/';

-- Silver bookings fact table, partitioned by check-in month.
CREATE EXTERNAL TABLE IF NOT EXISTS airbnb_lake.silver_bookings (
    booking_id    BIGINT,
    listing_id    BIGINT,
    guest_id      BIGINT,
    checkin_date  DATE,
    checkout_date DATE,
    nights        INT,
    amount        DOUBLE,
    status        STRING
)
PARTITIONED BY (checkin_month STRING)
STORED AS PARQUET
LOCATION 's3://ztm-data-engineering-bootcamp/silver/bookings/';

-- Silver transactions fact table, partitioned by transaction date.
CREATE EXTERNAL TABLE IF NOT EXISTS airbnb_lake.silver_transactions (
    txn_id         BIGINT,
    booking_id     BIGINT,
    ts             TIMESTAMP,
    amount         DOUBLE,
    currency       STRING,
    payment_method STRING,
    status         STRING
)
PARTITIONED BY (txn_date DATE)
STORED AS PARQUET
LOCATION 's3://ztm-data-engineering-bootcamp/silver/transactions/';

-- Gold: completed-booking revenue per listing per check-in month.
CREATE EXTERNAL TABLE IF NOT EXISTS airbnb_lake.gold_revenue_by_listing_month (
    id            BIGINT,
    name          STRING,
    neighbourhood STRING,
    num_bookings  BIGINT,
    total_nights  BIGINT,
    total_revenue DOUBLE
)
PARTITIONED BY (checkin_month STRING)
STORED AS PARQUET
LOCATION 's3://ztm-data-engineering-bootcamp/gold/revenue_by_listing_month/';

-- Gold: booking conversion and cancellation rates per listing.
CREATE EXTERNAL TABLE IF NOT EXISTS airbnb_lake.gold_booking_conversion (
    id                BIGINT,
    name              STRING,
    total_bookings    BIGINT,
    completed         BIGINT,
    confirmed         BIGINT,
    cancelled         BIGINT,
    conversion_rate   DOUBLE,
    cancellation_rate DOUBLE
)
STORED AS PARQUET
LOCATION 's3://ztm-data-engineering-bootcamp/gold/booking_conversion/';

-- Gold: transaction success rates by payment method.
CREATE EXTERNAL TABLE IF NOT EXISTS airbnb_lake.gold_transaction_success_rates (
    payment_method STRING,
    total_txns     BIGINT,
    succeeded      BIGINT,
    failed         BIGINT,
    refunded       BIGINT,
    settled_amount DOUBLE,
    success_rate   DOUBLE
)
STORED AS PARQUET
LOCATION 's3://ztm-data-engineering-bootcamp/gold/transaction_success_rates/';

-- After creating a partitioned table, load its partitions before querying:
MSCK REPAIR TABLE airbnb_lake.silver_reviews;
MSCK REPAIR TABLE airbnb_lake.silver_bookings;
MSCK REPAIR TABLE airbnb_lake.silver_transactions;
MSCK REPAIR TABLE airbnb_lake.gold_revenue_by_listing_month;

-- Example analyst queries against the gold layer.

-- Ten most reviewed listings.
SELECT name, neighbourhood, num_reviews
FROM airbnb_lake.gold_reviews_per_listing
ORDER BY num_reviews DESC
LIMIT 10;

-- Highest rated listings with at least five reviews.
SELECT name, avg_rating, num_reviews
FROM airbnb_lake.gold_avg_rating_per_listing
WHERE num_reviews >= 5
ORDER BY avg_rating DESC
LIMIT 10;

-- Review volume and satisfaction by neighbourhood.
SELECT neighbourhood, num_listings, num_reviews, avg_rating
FROM airbnb_lake.gold_reviews_per_neighbourhood
ORDER BY num_reviews DESC;

-- Top earning listings in a single month (partition pruning on checkin_month).
SELECT name, neighbourhood, num_bookings, total_revenue
FROM airbnb_lake.gold_revenue_by_listing_month
WHERE checkin_month = '2024-06'
ORDER BY total_revenue DESC
LIMIT 10;

-- Listings with the weakest booking conversion.
SELECT name, total_bookings, conversion_rate, cancellation_rate
FROM airbnb_lake.gold_booking_conversion
WHERE total_bookings >= 5
ORDER BY conversion_rate ASC
LIMIT 10;

-- Payment reliability by method.
SELECT payment_method, total_txns, success_rate, settled_amount
FROM airbnb_lake.gold_transaction_success_rates
ORDER BY total_txns DESC;
