#!/usr/bin/env bash
# 04: Register the gold and silver tables in Athena over the S3 parquet, load
# partitions, and run a sample analytics query.
#
# This adapts sql/athena_ddl.sql to your actual bucket. Each DDL statement and
# the sample query are submitted with `aws athena start-query-execution` and
# polled to completion. Results of the sample query are printed to the screen.
#
# Idempotent: uses CREATE DATABASE / TABLE IF NOT EXISTS and re-runs MSCK
# REPAIR, so running it repeatedly is safe.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/config.sh
source "${SCRIPT_DIR}/config.sh"

require_credentials

echo "=== 04 Athena: register tables and run a sample query ==="
print_config

# --- Helper: run one Athena statement and wait for it to finish --------------
# Usage: run_athena "<SQL>"  -> echoes the QueryExecutionId on success.
run_athena() {
  local sql="$1"
  local qid
  qid="$(aws athena start-query-execution \
    --region "${AWS_REGION}" \
    --query-string "${sql}" \
    --query-execution-context "Database=${ATHENA_DB}" \
    --result-configuration "OutputLocation=${ATHENA_OUTPUT}" \
    --query 'QueryExecutionId' --output text)"

  local state="RUNNING"
  for _ in $(seq 1 60); do
    state="$(aws athena get-query-execution \
      --region "${AWS_REGION}" \
      --query-execution-id "${qid}" \
      --query 'QueryExecution.Status.State' --output text)"
    case "${state}" in
      SUCCEEDED) echo "${qid}"; return 0 ;;
      FAILED|CANCELLED)
        local reason
        reason="$(aws athena get-query-execution --region "${AWS_REGION}" \
          --query-execution-id "${qid}" \
          --query 'QueryExecution.Status.StateChangeReason' --output text)"
        echo "ERROR: Athena query ${state}: ${reason}" >&2
        echo "  SQL: ${sql}" >&2
        return 1 ;;
    esac
    sleep 3
  done
  echo "ERROR: Athena query timed out (last state=${state})" >&2
  return 1
}

# The CREATE DATABASE statement is special: it must not run against a database
# context that does not exist yet, so submit it without the context.
echo "Creating Athena database ${ATHENA_DB}"
DB_QID="$(aws athena start-query-execution \
  --region "${AWS_REGION}" \
  --query-string "CREATE DATABASE IF NOT EXISTS ${ATHENA_DB}" \
  --result-configuration "OutputLocation=${ATHENA_OUTPUT}" \
  --query 'QueryExecutionId' --output text)"
for _ in $(seq 1 40); do
  DB_STATE="$(aws athena get-query-execution --region "${AWS_REGION}" \
    --query-execution-id "${DB_QID}" \
    --query 'QueryExecution.Status.State' --output text)"
  case "${DB_STATE}" in SUCCEEDED) break ;; FAILED|CANCELLED) echo "ERROR: create database ${DB_STATE}" >&2; exit 1 ;; esac
  sleep 3
done
echo "Database ready."

# --- Table DDL, adapted from sql/athena_ddl.sql with the real bucket ---------
echo "Registering silver_reviews (partitioned by review_month)"
run_athena "CREATE EXTERNAL TABLE IF NOT EXISTS ${ATHENA_DB}.silver_reviews (
    id BIGINT, listing_id BIGINT, reviewer_id BIGINT, reviewer_name STRING,
    rating INT, comments STRING, review_date DATE
) PARTITIONED BY (review_month STRING)
STORED AS PARQUET
LOCATION 's3://${BUCKET}/silver/reviews/'" >/dev/null

echo "Registering silver_listings"
run_athena "CREATE EXTERNAL TABLE IF NOT EXISTS ${ATHENA_DB}.silver_listings (
    id BIGINT, name STRING, host_id BIGINT, neighbourhood STRING,
    room_type STRING, price DOUBLE, minimum_nights INT
) STORED AS PARQUET
LOCATION 's3://${BUCKET}/silver/listings/'" >/dev/null

echo "Registering gold_reviews_per_listing"
run_athena "CREATE EXTERNAL TABLE IF NOT EXISTS ${ATHENA_DB}.gold_reviews_per_listing (
    id BIGINT, name STRING, neighbourhood STRING, num_reviews BIGINT
) STORED AS PARQUET
LOCATION 's3://${BUCKET}/gold/reviews_per_listing/'" >/dev/null

echo "Registering gold_avg_rating_per_listing"
run_athena "CREATE EXTERNAL TABLE IF NOT EXISTS ${ATHENA_DB}.gold_avg_rating_per_listing (
    id BIGINT, name STRING, avg_rating DOUBLE, num_reviews BIGINT
) STORED AS PARQUET
LOCATION 's3://${BUCKET}/gold/avg_rating_per_listing/'" >/dev/null

echo "Registering gold_reviews_per_neighbourhood"
run_athena "CREATE EXTERNAL TABLE IF NOT EXISTS ${ATHENA_DB}.gold_reviews_per_neighbourhood (
    neighbourhood STRING, num_reviews BIGINT, avg_rating DOUBLE, num_listings BIGINT
) STORED AS PARQUET
LOCATION 's3://${BUCKET}/gold/reviews_per_neighbourhood/'" >/dev/null

# --- Load partitions for the partitioned silver_reviews table ----------------
echo "Loading partitions: MSCK REPAIR TABLE ${ATHENA_DB}.silver_reviews"
run_athena "MSCK REPAIR TABLE ${ATHENA_DB}.silver_reviews" >/dev/null

# --- Run a sample analytics query and print the results ----------------------
echo ""
echo "Running sample query: ten most reviewed listings"
SAMPLE_QID="$(run_athena "SELECT name, neighbourhood, num_reviews
FROM ${ATHENA_DB}.gold_reviews_per_listing
ORDER BY num_reviews DESC
LIMIT 10")"

echo ""
echo "Results:"
aws athena get-query-results \
  --region "${AWS_REGION}" \
  --query-execution-id "${SAMPLE_QID}" \
  --query 'ResultSet.Rows[].Data[].VarCharValue' \
  --output text | tr '\t' '\n' | paste - - - | sed 's/^/  /'

echo ""
echo "SUCCESS: Athena database ${ATHENA_DB} is queryable."
echo "  Open the Athena console, pick database ${ATHENA_DB}, and query the"
echo "  gold_* tables. Query results are written to ${ATHENA_OUTPUT}"
echo "Your data lake is live on S3 + EMR Serverless + Athena."
echo "When you are done, run deploy/99_teardown.sh to stop all charges."
