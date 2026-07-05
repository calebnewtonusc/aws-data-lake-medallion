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

-- After creating a partitioned table, load its partitions before querying:
MSCK REPAIR TABLE airbnb_lake.silver_reviews;

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
