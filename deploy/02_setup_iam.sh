#!/usr/bin/env bash
# 02: Create the EMR Serverless job execution IAM role.
#
# EMR Serverless runs your Spark job under an IAM role (not your user). That
# role needs a trust policy allowing emr-serverless.amazonaws.com to assume it,
# plus permissions to read and write the lake bucket and to talk to Glue and
# Athena (so the same role can register and query tables if you extend it).
#
# Idempotent: re-running updates the inline policy and leaves an existing role
# in place. Prints the role ARN at the end.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/config.sh
source "${SCRIPT_DIR}/config.sh"

require_credentials

echo "=== 02 Set up EMR Serverless IAM execution role ==="
print_config

TRUST_POLICY=$(cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "emr-serverless.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}
JSON
)

# Inline policy: full read/write on this bucket only, plus Glue and Athena for
# table registration and queries. Scoped to the single lake bucket, not *.
INLINE_POLICY=$(cat <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "LakeBucketReadWrite",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::${BUCKET}",
        "arn:aws:s3:::${BUCKET}/*"
      ]
    },
    {
      "Sid": "GlueCatalogAccess",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:CreateDatabase",
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:BatchCreatePartition",
        "glue:CreatePartition"
      ],
      "Resource": "*"
    },
    {
      "Sid": "AthenaQuery",
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:GetWorkGroup"
      ],
      "Resource": "*"
    }
  ]
}
JSON
)

if aws iam get-role --role-name "${EMR_ROLE_NAME}" >/dev/null 2>&1; then
  echo "Role already exists: ${EMR_ROLE_NAME} (updating trust policy)"
  aws iam update-assume-role-policy \
    --role-name "${EMR_ROLE_NAME}" \
    --policy-document "${TRUST_POLICY}"
else
  echo "Creating role: ${EMR_ROLE_NAME}"
  aws iam create-role \
    --role-name "${EMR_ROLE_NAME}" \
    --assume-role-policy-document "${TRUST_POLICY}" \
    --description "EMR Serverless job execution role for the ${APP_NAME} data lake" \
    >/dev/null
fi

echo "Attaching inline policy: ${APP_NAME}-lake-access"
aws iam put-role-policy \
  --role-name "${EMR_ROLE_NAME}" \
  --policy-name "${APP_NAME}-lake-access" \
  --policy-document "${INLINE_POLICY}"

RESOLVED_ARN="$(aws iam get-role --role-name "${EMR_ROLE_NAME}" --query 'Role.Arn' --output text)"

echo ""
echo "SUCCESS: EMR Serverless execution role is ready."
echo "  EMR_ROLE_ARN = ${RESOLVED_ARN}"
echo ""
echo "This ARN matches the value config.sh derives automatically, so you do"
echo "not need to edit anything. If you customized EMR_ROLE_NAME, export the"
echo "ARN before the next step:"
echo "  export EMR_ROLE_ARN=\"${RESOLVED_ARN}\""
echo ""
echo "Note: IAM changes can take a few seconds to propagate. If step 03 fails"
echo "with an assume-role error on the first try, wait 10 seconds and re-run."
echo "Next: bash deploy/03_emr_serverless.sh"
