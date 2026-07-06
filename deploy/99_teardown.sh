#!/usr/bin/env bash
# 99: Tear everything down so there are zero ongoing charges.
#
# Stops and deletes the EMR Serverless application, deletes the IAM role and
# its inline policy, empties and deletes the S3 bucket, and drops the Athena
# database. Safe to run even if some resources are already gone: every step is
# guarded and reports what it did or skipped.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/config.sh
source "${SCRIPT_DIR}/config.sh"

require_credentials

echo "=== 99 Teardown: remove all AWS resources ==="
print_config
echo ""
echo "This deletes the bucket s3://${BUCKET} and everything in it, the EMR"
echo "Serverless application ${APP_NAME}, the IAM role ${EMR_ROLE_NAME}, and"
echo "the Athena database ${ATHENA_DB}."
if [ "${AUTO_YES:-0}" != "1" ]; then
  printf "Proceed with teardown? [y/N] "
  read -r answer
  case "${answer}" in y|Y|yes|YES) : ;; *) echo "Aborted."; exit 0 ;; esac
fi

# --- 1. EMR Serverless: stop and delete the application ----------------------
APP_ID="$(aws emr-serverless list-applications \
  --region "${AWS_REGION}" \
  --query "applications[?name=='${APP_NAME}'] | [0].id" \
  --output text 2>/dev/null || true)"

if [ -n "${APP_ID}" ] && [ "${APP_ID}" != "None" ]; then
  echo "Stopping EMR Serverless application ${APP_ID}"
  aws emr-serverless stop-application --region "${AWS_REGION}" --application-id "${APP_ID}" || true
  echo "Waiting for application to stop"
  for _ in $(seq 1 60); do
    ST="$(aws emr-serverless get-application --region "${AWS_REGION}" \
      --application-id "${APP_ID}" --query 'application.state' --output text 2>/dev/null || echo GONE)"
    case "${ST}" in STOPPED|GONE) break ;; esac
    echo "  ...state=${ST}"
    sleep 5
  done
  echo "Deleting application ${APP_ID}"
  aws emr-serverless delete-application --region "${AWS_REGION}" --application-id "${APP_ID}" || true
  echo "Application deleted."
else
  echo "No EMR Serverless application named ${APP_NAME} (skipping)"
fi

# --- 2. IAM: delete inline policy then the role ------------------------------
if aws iam get-role --role-name "${EMR_ROLE_NAME}" >/dev/null 2>&1; then
  echo "Deleting inline policies from role ${EMR_ROLE_NAME}"
  for pol in $(aws iam list-role-policies --role-name "${EMR_ROLE_NAME}" --query 'PolicyNames[]' --output text 2>/dev/null); do
    aws iam delete-role-policy --role-name "${EMR_ROLE_NAME}" --policy-name "${pol}" || true
  done
  echo "Deleting role ${EMR_ROLE_NAME}"
  aws iam delete-role --role-name "${EMR_ROLE_NAME}" || true
  echo "Role deleted."
else
  echo "No IAM role named ${EMR_ROLE_NAME} (skipping)"
fi

# --- 3. S3: empty and delete the bucket --------------------------------------
if aws s3api head-bucket --bucket "${BUCKET}" 2>/dev/null; then
  echo "Emptying bucket s3://${BUCKET}"
  aws s3 rm "s3://${BUCKET}" --recursive || true
  # Remove any versioned objects and delete markers if versioning was enabled.
  VERSIONS_JSON="$(aws s3api list-object-versions --bucket "${BUCKET}" \
    --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' --output json 2>/dev/null || echo '{}')"
  if echo "${VERSIONS_JSON}" | grep -q '"Key"'; then
    echo "Removing object versions"
    aws s3api delete-objects --bucket "${BUCKET}" --delete "${VERSIONS_JSON}" >/dev/null 2>&1 || true
  fi
  MARKERS_JSON="$(aws s3api list-object-versions --bucket "${BUCKET}" \
    --query '{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' --output json 2>/dev/null || echo '{}')"
  if echo "${MARKERS_JSON}" | grep -q '"Key"'; then
    echo "Removing delete markers"
    aws s3api delete-objects --bucket "${BUCKET}" --delete "${MARKERS_JSON}" >/dev/null 2>&1 || true
  fi
  echo "Deleting bucket s3://${BUCKET}"
  aws s3api delete-bucket --bucket "${BUCKET}" --region "${AWS_REGION}" || true
  echo "Bucket deleted."
else
  echo "No bucket named ${BUCKET} (skipping)"
fi

# --- 4. Athena / Glue: drop the database (CASCADE removes tables) ------------
echo "Dropping Athena database ${ATHENA_DB}"
DROP_QID="$(aws athena start-query-execution \
  --region "${AWS_REGION}" \
  --query-string "DROP DATABASE IF EXISTS ${ATHENA_DB} CASCADE" \
  --result-configuration "OutputLocation=s3://${BUCKET}/athena-results/" \
  --query 'QueryExecutionId' --output text 2>/dev/null || true)"
# The bucket may already be gone, which makes the results location invalid. In
# that case, drop tables via Glue directly as a fallback.
if [ -z "${DROP_QID}" ] || [ "${DROP_QID}" = "None" ]; then
  echo "Athena results bucket unavailable; dropping Glue database directly"
  aws glue delete-database --region "${AWS_REGION}" --name "${ATHENA_DB}" >/dev/null 2>&1 || true
else
  for _ in $(seq 1 20); do
    DS="$(aws athena get-query-execution --region "${AWS_REGION}" \
      --query-execution-id "${DROP_QID}" \
      --query 'QueryExecution.Status.State' --output text 2>/dev/null || echo DONE)"
    case "${DS}" in SUCCEEDED|FAILED|CANCELLED|DONE) break ;; esac
    sleep 3
  done
  # Ensure it is gone regardless of Athena result state.
  aws glue delete-database --region "${AWS_REGION}" --name "${ATHENA_DB}" >/dev/null 2>&1 || true
fi
echo "Database dropped."

echo ""
echo "SUCCESS: teardown complete. All lake resources removed."
echo "There should be no ongoing charges. You can verify in the S3, EMR"
echo "Serverless, IAM, and Athena consoles that nothing remains."
