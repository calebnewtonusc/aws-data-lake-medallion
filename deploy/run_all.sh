#!/usr/bin/env bash
# run_all: Run steps 01 through 04 in order, with a confirmation before each.
#
# This is the one-command path: `bash deploy/run_all.sh`. It pauses before each
# step so you can see what is about to happen. Set AUTO_YES=1 to skip the
# prompts and run everything unattended.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/config.sh
source "${SCRIPT_DIR}/config.sh"

require_credentials

echo "############################################################"
echo "#  AWS Medallion Data Lake: full deployment (01 -> 04)     #"
echo "############################################################"
print_config
echo ""
echo "This will create real AWS resources (S3, IAM, EMR Serverless, Athena)"
echo "in account ${AWS_ACCOUNT_ID}, region ${AWS_REGION}. Costs are small"
echo "(typically well under a few dollars). Run deploy/99_teardown.sh after"
echo "to remove everything."
echo ""

confirm() {
  local step="$1"
  if [ "${AUTO_YES:-0}" = "1" ]; then
    echo ">>> Running ${step} (AUTO_YES=1)"
    return 0
  fi
  printf ">>> Run %s? [y/N] " "${step}"
  read -r answer
  case "${answer}" in
    y|Y|yes|YES) return 0 ;;
    *) echo "Skipped ${step}."; return 1 ;;
  esac
}

if confirm "01_create_bucket.sh (create bucket, upload data + job)"; then
  bash "${SCRIPT_DIR}/01_create_bucket.sh"
fi

if confirm "02_setup_iam.sh (create EMR execution role)"; then
  bash "${SCRIPT_DIR}/02_setup_iam.sh"
fi

if confirm "03_emr_serverless.sh (run the Spark medallion job)"; then
  bash "${SCRIPT_DIR}/03_emr_serverless.sh"
fi

if confirm "04_athena.sh (register tables and query)"; then
  bash "${SCRIPT_DIR}/04_athena.sh"
fi

echo ""
echo "############################################################"
echo "#  Deployment complete.                                    #"
echo "#  Your resume line is now literally true.                 #"
echo "#  Remember: bash deploy/99_teardown.sh to stop charges.   #"
echo "############################################################"
