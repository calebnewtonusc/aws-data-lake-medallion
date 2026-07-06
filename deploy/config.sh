#!/usr/bin/env bash
# Shared configuration for the AWS medallion data lake deployment.
#
# Every deploy script sources this file. Edit the values below if you want a
# specific bucket name or region; the defaults are sensible and derive a
# globally-unique bucket name from your AWS account id so the scripts work out
# of the box after a single `aws configure`.
#
# Nothing here makes an AWS call except the account-id lookup used to build a
# unique default bucket name. That lookup is skipped if you set BUCKET yourself.

# --- Editable variables -----------------------------------------------------

# AWS region for every resource. us-east-1 is the cheapest and simplest.
export AWS_REGION="${AWS_REGION:-us-east-1}"

# Logical name used for the EMR Serverless application and the IAM role.
export APP_NAME="${APP_NAME:-airbnb-medallion}"

# Glue / Athena database that the gold and silver tables register into.
export ATHENA_DB="${ATHENA_DB:-airbnb_lake}"

# EMR Serverless release. 7.2.0 is a current, stable Spark release label.
export EMR_RELEASE_LABEL="${EMR_RELEASE_LABEL:-emr-7.2.0}"

# --- Derived / discovered values --------------------------------------------

# Resolve the AWS account id once so we can build a unique bucket name and the
# IAM role ARN. If credentials are not configured yet this stays empty and the
# scripts that need it will print a clear message.
if [ -z "${AWS_ACCOUNT_ID:-}" ]; then
  AWS_ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
  export AWS_ACCOUNT_ID
fi

# S3 bucket that holds the whole lake (bronze, silver, gold, apps, logs,
# athena-results). Bucket names are globally unique, so we suffix the account
# id. Override by exporting BUCKET before running, or by editing this line.
if [ -z "${BUCKET:-}" ]; then
  if [ -n "${AWS_ACCOUNT_ID}" ]; then
    export BUCKET="${APP_NAME}-lake-${AWS_ACCOUNT_ID}"
  else
    export BUCKET=""
  fi
fi

# Name of the EMR Serverless job execution role and the ARN we expect it at.
export EMR_ROLE_NAME="${EMR_ROLE_NAME:-${APP_NAME}-emr-exec-role}"
if [ -z "${EMR_ROLE_ARN:-}" ]; then
  if [ -n "${AWS_ACCOUNT_ID}" ]; then
    export EMR_ROLE_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:role/${EMR_ROLE_NAME}"
  else
    export EMR_ROLE_ARN=""
  fi
fi

# S3 location Athena writes query results to. Athena requires this.
export ATHENA_OUTPUT="${ATHENA_OUTPUT:-s3://${BUCKET}/athena-results/}"

# Key under which the EMR Spark job script is uploaded, and its full S3 URI.
export EMR_JOB_KEY="${EMR_JOB_KEY:-apps/medallion_emr.py}"
export EMR_JOB_S3_URI="${EMR_JOB_S3_URI:-s3://${BUCKET}/${EMR_JOB_KEY}}"

# S3 prefixes for each medallion layer and for EMR logs.
export BRONZE_URI="s3://${BUCKET}/bronze"
export SILVER_URI="s3://${BUCKET}/silver"
export GOLD_URI="s3://${BUCKET}/gold"
export LOG_URI="s3://${BUCKET}/logs"

# Ingest date partition used when uploading the raw bronze data.
export INGEST_DATE="${INGEST_DATE:-$(date +%F)}"

# Repo root, resolved relative to this file, so scripts can find src and jobs.
CONFIG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${CONFIG_DIR}/.." && pwd)"
export REPO_ROOT

# --- Helpers ----------------------------------------------------------------

# Fail fast with a clear message if credentials are not configured.
require_credentials() {
  if [ -z "${AWS_ACCOUNT_ID}" ]; then
    echo "ERROR: AWS credentials are not configured (aws sts get-caller-identity failed)." >&2
    echo "       Run 'aws configure' first. See docs/DEPLOY-RUNBOOK.md step 2." >&2
    exit 1
  fi
}

# Print the resolved configuration so the user can eyeball it before deploying.
print_config() {
  echo "----------------------------------------------------------------"
  echo " AWS_REGION        = ${AWS_REGION}"
  echo " AWS_ACCOUNT_ID    = ${AWS_ACCOUNT_ID:-<not configured>}"
  echo " APP_NAME          = ${APP_NAME}"
  echo " BUCKET            = ${BUCKET:-<pending account id>}"
  echo " EMR_ROLE_NAME     = ${EMR_ROLE_NAME}"
  echo " EMR_ROLE_ARN      = ${EMR_ROLE_ARN:-<pending account id>}"
  echo " EMR_RELEASE_LABEL = ${EMR_RELEASE_LABEL}"
  echo " EMR_JOB_S3_URI    = ${EMR_JOB_S3_URI}"
  echo " ATHENA_DB         = ${ATHENA_DB}"
  echo " ATHENA_OUTPUT     = ${ATHENA_OUTPUT}"
  echo " INGEST_DATE       = ${INGEST_DATE}"
  echo "----------------------------------------------------------------"
}
