#!/usr/bin/env bash
# 01: Create the S3 lake bucket and upload the raw Airbnb data plus the EMR
# Spark job script.
#
# Idempotent: re-running skips a bucket that already exists and simply
# re-uploads the objects. Safe to run as many times as you like.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/config.sh
source "${SCRIPT_DIR}/config.sh"

require_credentials

echo "=== 01 Create S3 bucket and upload data ==="
print_config

# --- 1. Create the bucket (handle the us-east-1 LocationConstraint quirk) ----
if aws s3api head-bucket --bucket "${BUCKET}" 2>/dev/null; then
  echo "Bucket already exists: s3://${BUCKET} (skipping create)"
else
  echo "Creating bucket: s3://${BUCKET} in ${AWS_REGION}"
  if [ "${AWS_REGION}" = "us-east-1" ]; then
    # us-east-1 rejects an explicit LocationConstraint.
    aws s3api create-bucket --bucket "${BUCKET}" --region us-east-1
  else
    aws s3api create-bucket \
      --bucket "${BUCKET}" \
      --region "${AWS_REGION}" \
      --create-bucket-configuration "LocationConstraint=${AWS_REGION}"
  fi
  echo "Bucket created."
fi

# --- 2. Optional versioning (uncomment to keep object history) ---------------
# Versioning is off by default to keep teardown simple and costs at zero. To
# enable it, set ENABLE_VERSIONING=1 in the environment before running.
if [ "${ENABLE_VERSIONING:-0}" = "1" ]; then
  echo "Enabling bucket versioning"
  aws s3api put-bucket-versioning \
    --bucket "${BUCKET}" \
    --versioning-configuration Status=Enabled
else
  echo "Versioning left disabled (set ENABLE_VERSIONING=1 to turn it on)"
fi

# --- 3. Generate the raw bronze sample data if not already present -----------
RAW_DIR="${REPO_ROOT}/data/raw"
LISTINGS_JSON="${RAW_DIR}/listings.json"
REVIEWS_JSON="${RAW_DIR}/reviews.json"

if [ -f "${LISTINGS_JSON}" ] && [ -f "${REVIEWS_JSON}" ]; then
  echo "Raw data already generated at ${RAW_DIR} (skipping generation)"
else
  echo "Generating raw Airbnb sample data into ${RAW_DIR}"
  mkdir -p "${RAW_DIR}"
  # Prefer a project virtualenv python if one exists, else fall back to python3.
  PYTHON_BIN="python3"
  for candidate in "${REPO_ROOT}/.venv/bin/python" "${REPO_ROOT}/venv/bin/python"; do
    if [ -x "${candidate}" ]; then PYTHON_BIN="${candidate}"; break; fi
  done
  ( cd "${REPO_ROOT}" && "${PYTHON_BIN}" scripts/generate_events.py \
      --num-listings "${NUM_LISTINGS:-400}" \
      --seed "${SEED:-42}" \
      --listings-out "${LISTINGS_JSON}" \
      --reviews-out "${REVIEWS_JSON}" )
  echo "Generated $(wc -l < "${LISTINGS_JSON}" | tr -d ' ') listing lines and $(wc -l < "${REVIEWS_JSON}" | tr -d ' ') review lines"
fi

# --- 4. Upload the raw data to the bronze layer, partitioned by ingest date --
LISTINGS_KEY="bronze/listings/ingest_date=${INGEST_DATE}/listings.json"
REVIEWS_KEY="bronze/reviews/ingest_date=${INGEST_DATE}/reviews.json"

echo "Uploading raw listings to s3://${BUCKET}/${LISTINGS_KEY}"
aws s3 cp "${LISTINGS_JSON}" "s3://${BUCKET}/${LISTINGS_KEY}" --content-type application/json

echo "Uploading raw reviews to s3://${BUCKET}/${REVIEWS_KEY}"
aws s3 cp "${REVIEWS_JSON}" "s3://${BUCKET}/${REVIEWS_KEY}" --content-type application/json

# --- 5. Upload the EMR Serverless Spark job script ---------------------------
echo "Uploading EMR Spark job to ${EMR_JOB_S3_URI}"
aws s3 cp "${REPO_ROOT}/jobs/medallion_emr.py" "${EMR_JOB_S3_URI}"

echo ""
echo "SUCCESS: bucket ready and objects uploaded."
echo "  Bronze listings: s3://${BUCKET}/${LISTINGS_KEY}"
echo "  Bronze reviews:  s3://${BUCKET}/${REVIEWS_KEY}"
echo "  EMR job:         ${EMR_JOB_S3_URI}"
echo "Next: bash deploy/02_setup_iam.sh"
