#!/usr/bin/env bash
# 03: Create (or reuse) an EMR Serverless Spark application, then submit the
# medallion job as a job run and poll it to completion.
#
# The job reads the bronze JSON from S3, builds silver and gold, and writes
# parquet back to S3. This is the step that makes "Built an AWS data lake using
# S3, EMR, and Athena" literally true.
#
# Idempotent: reuses an application named ${APP_NAME} if one already exists;
# otherwise creates one. Each run submits a fresh job run.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/config.sh
source "${SCRIPT_DIR}/config.sh"

require_credentials

echo "=== 03 EMR Serverless: create app and run the medallion job ==="
print_config

if [ -z "${EMR_ROLE_ARN}" ]; then
  echo "ERROR: EMR_ROLE_ARN is empty. Run deploy/02_setup_iam.sh first." >&2
  exit 1
fi

# --- 1. Find an existing application by name, or create one ------------------
echo "Looking for an existing EMR Serverless application named ${APP_NAME}"
APP_ID="$(aws emr-serverless list-applications \
  --region "${AWS_REGION}" \
  --query "applications[?name=='${APP_NAME}'] | [0].id" \
  --output text 2>/dev/null || true)"

if [ -n "${APP_ID}" ] && [ "${APP_ID}" != "None" ]; then
  echo "Reusing application: ${APP_ID}"
else
  echo "Creating EMR Serverless application (${EMR_RELEASE_LABEL}, type SPARK)"
  APP_ID="$(aws emr-serverless create-application \
    --region "${AWS_REGION}" \
    --name "${APP_NAME}" \
    --release-label "${EMR_RELEASE_LABEL}" \
    --type SPARK \
    --query 'applicationId' \
    --output text)"
  echo "Created application: ${APP_ID}"
fi

# --- 2. Make sure the application is STARTED ---------------------------------
get_app_state() {
  aws emr-serverless get-application \
    --region "${AWS_REGION}" \
    --application-id "${APP_ID}" \
    --query 'application.state' --output text
}

STATE="$(get_app_state)"
echo "Application state: ${STATE}"
if [ "${STATE}" != "STARTED" ]; then
  echo "Starting application ${APP_ID}"
  aws emr-serverless start-application --region "${AWS_REGION}" --application-id "${APP_ID}"
  echo "Waiting for application to reach STARTED"
  for _ in $(seq 1 60); do
    STATE="$(get_app_state)"
    if [ "${STATE}" = "STARTED" ]; then break; fi
    echo "  ...state=${STATE}"
    sleep 5
  done
  if [ "${STATE}" != "STARTED" ]; then
    echo "ERROR: application did not reach STARTED (state=${STATE})." >&2
    exit 1
  fi
fi
echo "Application STARTED."

# --- 3. Submit the medallion job run -----------------------------------------
JOB_DRIVER=$(cat <<JSON
{
  "sparkSubmit": {
    "entryPoint": "${EMR_JOB_S3_URI}",
    "entryPointArguments": [
      "--bronze", "${BRONZE_URI}",
      "--silver", "${SILVER_URI}",
      "--gold", "${GOLD_URI}"
    ],
    "sparkSubmitParameters": "--conf spark.executor.cores=2 --conf spark.executor.memory=4g --conf spark.driver.cores=1 --conf spark.driver.memory=4g --conf spark.executor.instances=2"
  }
}
JSON
)

CONFIG_OVERRIDES=$(cat <<JSON
{
  "monitoringConfiguration": {
    "s3MonitoringConfiguration": { "logUri": "${LOG_URI}" }
  }
}
JSON
)

echo "Submitting job run to application ${APP_ID}"
JOB_RUN_ID="$(aws emr-serverless start-job-run \
  --region "${AWS_REGION}" \
  --application-id "${APP_ID}" \
  --execution-role-arn "${EMR_ROLE_ARN}" \
  --name "${APP_NAME}-medallion-$(date +%Y%m%d-%H%M%S)" \
  --job-driver "${JOB_DRIVER}" \
  --configuration-overrides "${CONFIG_OVERRIDES}" \
  --query 'jobRunId' --output text)"

echo "Job run submitted: ${JOB_RUN_ID}"

# --- 4. Poll the job run until SUCCESS or FAILED -----------------------------
echo "Polling job run (this typically takes 2 to 5 minutes)..."
JOB_STATE="SUBMITTED"
for _ in $(seq 1 120); do
  JOB_STATE="$(aws emr-serverless get-job-run \
    --region "${AWS_REGION}" \
    --application-id "${APP_ID}" \
    --job-run-id "${JOB_RUN_ID}" \
    --query 'jobRun.state' --output text)"
  echo "  ...job state=${JOB_STATE}"
  case "${JOB_STATE}" in
    SUCCESS|FAILED|CANCELLED) break ;;
  esac
  sleep 10
done

echo ""
echo "Final job run state: ${JOB_STATE}"
STATE_DETAILS="$(aws emr-serverless get-job-run \
  --region "${AWS_REGION}" \
  --application-id "${APP_ID}" \
  --job-run-id "${JOB_RUN_ID}" \
  --query 'jobRun.stateDetails' --output text 2>/dev/null || true)"
[ -n "${STATE_DETAILS}" ] && [ "${STATE_DETAILS}" != "None" ] && echo "State details: ${STATE_DETAILS}"

DRIVER_LOGS="${LOG_URI}/applications/${APP_ID}/jobs/${JOB_RUN_ID}/SPARK_DRIVER/"
echo ""
echo "Driver logs (stdout.gz / stderr.gz) are at:"
echo "  ${DRIVER_LOGS}"
echo "View them with:"
echo "  aws s3 cp ${DRIVER_LOGS}stdout.gz - | gunzip"

if [ "${JOB_STATE}" != "SUCCESS" ]; then
  echo ""
  echo "Job did not succeed. Read the driver stderr above for the cause." >&2
  exit 1
fi

echo ""
echo "SUCCESS: silver and gold parquet written under s3://${BUCKET}/"
echo "  Silver: ${SILVER_URI}/"
echo "  Gold:   ${GOLD_URI}/"
echo "Next: bash deploy/04_athena.sh"
