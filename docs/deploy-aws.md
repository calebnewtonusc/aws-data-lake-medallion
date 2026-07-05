# Deploying to real AWS

The local run uses moto to mock S3 so the pipeline is fully offline. The same
code targets a real AWS account by changing configuration only, never logic.
This mirrors the ZTM Data Engineering course, which runs the Spark job on EMR
Serverless against S3 and queries the result with Athena.

## 1. Configure credentials

Follow the course setup: create an IAM user with programmatic access and store
the keys in the default credential file.

```ini
# ~/.aws/credentials
[default]
aws_access_key_id = YOUR_ACCESS_KEY_ID
aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
```

Verify access:

```sh
aws s3 ls
```

## 2. Point the pipeline at a real bucket

Every path is derived from environment variables read in `src/config.py`. Set
them, then run the bronze landing exactly as in the local flow. Because the
boto3 client resolves the standard credential chain, no code changes are
needed.

```sh
export LAKE_BUCKET=your-data-lake-bucket
export AWS_REGION=us-east-1
aws s3 mb "s3://$LAKE_BUCKET"
```

The bronze landing functions in `src/bronze.py` already call the real S3 API
through boto3. Removing the `@mock_aws` decorator from `src/run_pipeline.py`
(or invoking `bronze.land_dataset` directly) lands raw JSON into the real
bucket under `bronze/<dataset>/ingest_date=<date>/`.

## 3. Run the Spark jobs on EMR Serverless

Locally, Spark reads and writes the mirrored lake root because the Hadoop s3a
client against an in-process moto endpoint is unreliable. On real AWS, Spark
reads and writes S3 directly. Package the silver and gold jobs and submit them
to an EMR Serverless application. This matches the course payload structure.

```json
{
  "sparkSubmit": {
    "entryPoint": "s3://your-data-lake-bucket/apps/run_silver_gold.py",
    "entryPointArguments": [
      "--reviews",
      "s3://your-data-lake-bucket/bronze/reviews/",
      "--listings",
      "s3://your-data-lake-bucket/bronze/listings/",
      "--silver",
      "s3://your-data-lake-bucket/silver/",
      "--gold",
      "s3://your-data-lake-bucket/gold/"
    ]
  }
}
```

Point Spark reads and writes at the `s3://` URIs from `src/config.py` instead
of the local lake root. With the `hadoop-aws` jar on the classpath, the
`s3a://` scheme resolves the same IAM credentials configured above.

Attach a monitoring configuration so driver and executor logs land in S3:

```json
{
  "monitoringConfiguration": {
    "s3MonitoringConfiguration": {
      "logUri": "s3://your-data-lake-bucket/logs"
    }
  }
}
```

## 4. Query the gold layer with Athena

Run the statements in `sql/athena_ddl.sql` in the Athena query editor, or point
a Glue crawler at the `silver/` and `gold/` prefixes. Set an Athena query
result location in S3, run `MSCK REPAIR TABLE` on partitioned tables, then
query the gold tables directly.

## Cost and cleanup

EMR Serverless and Athena bill per usage. Delete the EMR application, empty the
bucket, and remove the IAM keys when finished to avoid charges.
