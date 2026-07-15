# AWS Data Lake Deploy Runbook

This runbook takes you from an empty AWS account to a real, working data lake
on S3, EMR Serverless, and Athena, running the same Airbnb medallion pipeline
you already run locally. Follow it top to bottom. When you finish, the resume
line "Built an AWS data lake using S3, EMR, and Athena" is literally true,
because you will have done exactly that.

You do not need to be an AWS expert. Every command is written out. The scripts
are idempotent, which means running one twice is safe: it either skips work
already done or replaces it cleanly. When you are finished, one teardown
command removes everything so there are no ongoing charges.

Total hands-on time is about 15 minutes. The EMR job itself runs for a few
minutes on its own while you watch the state print.

---

## What you will build

A single S3 bucket holds the whole lake in three layers:

- bronze: the raw Airbnb listings and reviews, landed as JSON, exactly as the source produced them.
- silver: cleaned, validated, typed, deduplicated parquet, produced by a Spark job on EMR Serverless.
- gold: three business aggregate tables (reviews per listing, average rating per listing, reviews per neighbourhood), also parquet.

Athena then queries the gold and silver tables directly in place, with SQL, no
data copying. That is the classic medallion data lake on AWS.

The scripts live in the `deploy/` folder:

| Script                 | What it does                                                                                           |
| ---------------------- | ------------------------------------------------------------------------------------------------------ |
| `config.sh`            | Shared variables. Sourced by the others. You rarely edit it.                                           |
| `01_create_bucket.sh`  | Creates the S3 bucket, generates the raw data if needed, uploads the raw data and the Spark job to S3. |
| `02_setup_iam.sh`      | Creates the IAM role EMR Serverless runs your job under.                                               |
| `03_emr_serverless.sh` | Creates the EMR Serverless app, runs the Spark medallion job, waits for it to finish.                  |
| `04_athena.sh`         | Registers the tables in Athena, loads partitions, runs a sample query, prints results.                 |
| `run_all.sh`           | Runs 01 through 04 in order, pausing before each.                                                      |
| `99_teardown.sh`       | Deletes everything so charges stop.                                                                    |

---

## Step 0: Create or log into an AWS account

1. Go to https://aws.amazon.com and either sign in or choose "Create an AWS Account".
2. Creating an account requires an email, a password, and a credit card. AWS uses the card for verification and for anything beyond the free tier. For this project the cost is tiny (see the cost note in step 6), but a card is required.
3. Finish sign-up and log into the AWS Management Console. You now have a root account. For real work you should not use the root account day to day, which is why step 1 creates a separate user.

---

## Step 1: Create an IAM user with programmatic access

The scripts talk to AWS through the AWS CLI, which needs an access key. Create
a dedicated user for this.

1. In the AWS Console search bar, type IAM and open the IAM service.
2. In the left menu choose Users, then Create user.
3. Name it something like `data-lake-deployer`. Click Next.
4. On the permissions page choose "Attach policies directly" and attach these AWS managed policies. Search each name and check the box:
   - `AmazonS3FullAccess` (create and use the bucket)
   - `AmazonAthenaFullAccess` (create the database, run queries)
   - `AWSGlueConsoleFullAccess` (Athena stores table definitions in the Glue Data Catalog)
   - `AmazonEMRServerlessFullAccess` (create and run the EMR Serverless app)
   - `IAMFullAccess` (needed only so the scripts can create the EMR execution role and, later, delete it; you can remove this after teardown)
5. Click Next, then Create user.
6. Open the user you just made. Go to the "Security credentials" tab. Under "Access keys" choose "Create access key". Pick "Command Line Interface (CLI)", acknowledge the note, and create it.
7. You will see an Access key ID and a Secret access key. Copy both now. The secret is shown only once. If you lose it, delete the key and make a new one.

Least-privilege alternative, optional: the five managed policies above are the
simple, get-it-working choice. If you prefer tighter permissions, you can
replace them with a custom policy scoped to just this bucket, the specific EMR
Serverless and Athena actions, and only the `iam:*Role*` and `iam:*RolePolicy`
actions on a role named `airbnb-medallion-emr-exec-role`. The managed policies
are perfectly fine for a personal project you tear down afterward.

---

## Step 2: Run aws configure

The AWS CLI v2 is already installed on this machine. Confirm it and set your
region to make sure everything lands in the same place. If `aws` is not found,
add Homebrew to your PATH first with `export PATH="/opt/homebrew/bin:$PATH"`.

```sh
aws --version
aws configure
```

`aws configure` asks four things. Answer them like this:

- AWS Access Key ID: paste the Access key ID from step 1.
- AWS Secret Access Key: paste the Secret access key from step 1.
- Default region name: `us-east-1`
- Default output format: `json`

Verify it worked:

```sh
aws sts get-caller-identity
```

You should see your account id and the user ARN. If you see an error, the keys
are wrong or not saved; re-run `aws configure`.

---

## Step 3: Edit deploy/config.sh if you want (optional)

You do not have to change anything. By default the bucket name is derived from
your account id so it is globally unique (`airbnb-medallion-lake-<accountid>`),
the region is us-east-1, and the Athena database is `airbnb_lake`.

If you want a custom bucket name or region, open `deploy/config.sh` and edit
the values at the top, or export them before running, for example:

```sh
export AWS_REGION=us-east-1
export BUCKET=my-custom-lake-name
```

To see the exact configuration that will be used, source the file and print it:

```sh
cd /Users/joelnewton/Desktop/DE-Portfolio/aws-data-lake-medallion
source deploy/config.sh
print_config
```

---

## Step 4: Run the scripts

You can run everything at once, or step by step. Run from the repo root:

```sh
cd /Users/joelnewton/Desktop/DE-Portfolio/aws-data-lake-medallion
```

### The one-command path

```sh
bash deploy/run_all.sh
```

It prints the configuration, then pauses before each of the four steps and
asks `Run 01_...? [y/N]`. Type `y` and press Enter for each. To run it fully
unattended, set `AUTO_YES=1` first:

```sh
AUTO_YES=1 bash deploy/run_all.sh
```

### The step-by-step path

Run these in order. What each does and what success looks like:

```sh
bash deploy/01_create_bucket.sh
```

Creates the S3 bucket (handling the us-east-1 special case where AWS rejects an
explicit location), generates the raw Airbnb sample data into `data/raw/` if it
is not already there, and uploads the raw JSON to `bronze/` and the Spark job
to `apps/medallion_emr.py`. Success looks like: `SUCCESS: bucket ready and
objects uploaded.` with the three S3 paths listed.

```sh
bash deploy/02_setup_iam.sh
```

Creates the IAM role that EMR Serverless assumes to run your job, with a trust
policy for `emr-serverless.amazonaws.com` and an inline policy granting access
to your bucket plus Glue and Athena. Success looks like: `SUCCESS: EMR
Serverless execution role is ready.` and prints the role ARN. If step 03 later
fails with an assume-role error on the very first try, wait about 10 seconds
for IAM to propagate and re-run step 03; this is normal AWS behavior.

```sh
bash deploy/03_emr_serverless.sh
```

Creates (or reuses) an EMR Serverless Spark application on release emr-7.2.0,
starts it, submits the medallion job pointing at your bronze data, and polls
until the job reaches SUCCESS or FAILED. This is the step that actually runs
Spark on AWS. It usually takes 2 to 5 minutes. You will see the app state climb
to STARTED, then the job state print repeatedly until it shows SUCCESS.
Success looks like: `Final job run state: SUCCESS` followed by
`SUCCESS: silver and gold parquet written under s3://...`. It also prints the
S3 location of the driver logs so you can read them if anything goes wrong.

```sh
bash deploy/04_athena.sh
```

Creates the Athena database, registers the silver and gold tables over the S3
parquet, runs `MSCK REPAIR TABLE` to load the partitioned reviews table, then
runs a sample query (the ten most reviewed listings) and prints the rows.
Success looks like: a small table of listing names and review counts, then
`SUCCESS: Athena database airbnb_lake is queryable.`

At this point your data lake is live end to end.

---

## Step 5: See your results in the AWS consoles

Log into the AWS Console and look at what you built. Make sure the region in
the top right corner is N. Virginia (us-east-1), or whatever region you chose.

- S3: open the S3 service, click your bucket. You will see `bronze/`, `silver/`, `gold/`, `apps/`, `logs/`, and `athena-results/` prefixes. Drill into `gold/reviews_per_listing/` to see the parquet files the Spark job wrote.
- EMR Serverless: open the EMR service, choose "EMR Serverless" in the left menu, then "Manage applications". Click the `airbnb-medallion` application and look at its Job runs. Click your run to see its state, timing, and a link to the driver logs. This is the proof that Spark ran on EMR.
- Athena: open the Athena service and go to the Query editor. On the left, pick the `airbnb_lake` database. You will see the five tables. If the editor asks you to set a query result location first, point it at `s3://<your-bucket>/athena-results/`. Then run a query, for example:

  ```sql
  SELECT neighbourhood, num_listings, num_reviews, avg_rating
  FROM airbnb_lake.gold_reviews_per_neighbourhood
  ORDER BY num_reviews DESC;
  ```

  You will get results back in a second or two. That is Athena reading your gold parquet in place.

---

## Step 5b: Scale the lake to 250+ GB on AWS (optional)

The steps above build the medallion lake at the small validated sample size,
because a laptop does not have hundreds of GB of free disk. The architecture,
however, is designed to scale, and the scale-up is a single parameterized EMR
job: `jobs/scale_generate_emr.py`. It generates the four source domains
(listings, reviews, bookings, transactions) at any target size and writes them
as partitioned, compacted, sorted Parquet directly into S3, which has no local
disk limit. This is how the "250+ GB AWS data lake" is produced for real.

It writes the same optimized physical layout the benchmark measured: each fact
partitioned by month, compacted to right-sized files, sorted within partition so
Parquet row-group statistics enable predicate pushdown. So the 250 GB lake on
AWS carries the same query-optimization properties proven locally, at production
scale.

### Upload the scale job

```sh
cd /Users/joelnewton/Desktop/DE-Portfolio/aws-data-lake-medallion
source deploy/config.sh
aws s3 cp jobs/scale_generate_emr.py "s3://${BUCKET}/apps/scale_generate_emr.py"
```

### Submit it to EMR Serverless

Reuse the EMR Serverless application and execution role that step 03 created.
Get their ids and submit the job, pointing `--target-gb` at the size you want
(250 for the resume-scale lake) and `--out` at a `scale/` prefix in your bucket:

```sh
APP_ID=$(aws emr-serverless list-applications \
  --query "applications[?name=='airbnb-medallion'].id | [0]" --output text)
ROLE_ARN=$(aws iam get-role --role-name airbnb-medallion-emr-exec-role \
  --query 'Role.Arn' --output text)

aws emr-serverless start-job-run \
  --application-id "$APP_ID" \
  --execution-role-arn "$ROLE_ARN" \
  --name "scale-250gb" \
  --job-driver "{
    \"sparkSubmit\": {
      \"entryPoint\": \"s3://${BUCKET}/apps/scale_generate_emr.py\",
      \"entryPointArguments\": [
        \"--out\", \"s3://${BUCKET}/scale\",
        \"--target-gb\", \"250\",
        \"--domain\", \"all\"
      ],
      \"sparkSubmitParameters\": \"--conf spark.executor.cores=4 --conf spark.executor.memory=12g --conf spark.executor.instances=20 --conf spark.driver.memory=8g\"
    }
  }"
```

The job prints the row counts it derives from `--target-gb` before writing (250
GB is on the order of five billion transaction rows), then writes each domain.
Because transactions carry about 80% of the volume, you can also generate one
domain at a time with `--domain transactions|bookings|reviews|listings` and a
smaller `--target-gb` to spread the run out.

### Point Athena at the 250 GB tables

Register external tables over `s3://<bucket>/scale/<domain>/` and load partitions
the same way `04_athena.sh` does for the sample tables, using the DDL patterns in
`sql/athena_ddl.sql`. Once the partitions are loaded, the same date-bounded and
window-function queries in the benchmark run against the full 250 GB lake, and
Athena only scans the month partitions each query touches.

### Cost and cleanup for the scale run

250 GB of Parquet in S3 is about 6 dollars a month of storage, and the one EMR
Serverless generation run is billed only while it runs (a larger multi-worker
job, so budget a few dollars, not cents). This is real money, so tear it down
when you are done: `aws s3 rm "s3://${BUCKET}/scale" --recursive` removes the
250 GB, and `deploy/99_teardown.sh` removes everything else. Only run the 250 GB
generation when you specifically want the full-scale lake live; the sample lake
from steps 01 to 04 is enough to see the architecture end to end.

---

## Step 6: A realistic cost note

For this tiny dataset the total cost of running the whole thing once is
typically well under a few dollars, and often just a few cents. Here is the
honest breakdown:

- S3: you are storing a handful of megabytes. Storage is about 0.023 dollars per GB per month, so this is a fraction of a cent. Requests are pennies at most.
- Athena: billed at about 5 dollars per terabyte scanned, with a 10 MB minimum per query. Your gold tables are kilobytes, so each query rounds to essentially nothing. A dozen queries is still pennies.
- EMR Serverless: this is the only part that can add up, and it is still small. You are billed per vCPU-hour and per GB-hour only while the job is actually running, plus a brief pre-initialized capacity window. The medallion job runs for a few minutes on a couple of small workers, so a single run is typically well under a dollar, often in the range of ten to fifty cents. It does not bill while idle after the job finishes, and teardown removes the application entirely.

Free tier notes: new AWS accounts get 5 GB of S3 storage free for the first 12
months, which fully covers this project. Athena and EMR Serverless are not part
of the always-free tier, but the amounts here are so small they stay in the
pennies range regardless. The single most important habit is to run teardown
when you are done so nothing lingers.

If you want to double check what you spent, open the Billing and Cost
Management console a day later and look at Cost Explorer. You will likely see
under a dollar total.

---

## Step 7: IMPORTANT, tear everything down to stop all charges

When you are finished exploring, run teardown. This is the step that guarantees
you are not paying for anything.

```sh
cd /Users/joelnewton/Desktop/DE-Portfolio/aws-data-lake-medallion
bash deploy/99_teardown.sh
```

It asks you to confirm, then stops and deletes the EMR Serverless application,
deletes the IAM role and its policy, empties and deletes the S3 bucket, and
drops the Athena database. To run it without the prompt, use
`AUTO_YES=1 bash deploy/99_teardown.sh`. Success looks like: `SUCCESS: teardown
complete. All lake resources removed.`

After teardown, you can verify in the S3, EMR Serverless, IAM, and Athena
consoles that nothing remains. If you attached `IAMFullAccess` to your deployer
user only for this project, you can remove that policy from the user now too.

You can re-run the whole thing any time by starting again at step 4. The
scripts will recreate everything from scratch.

---

## Quick reference: run it, then kill it

```sh
cd /Users/joelnewton/Desktop/DE-Portfolio/aws-data-lake-medallion
aws configure                        # once, with your keys and us-east-1
AUTO_YES=1 bash deploy/run_all.sh    # build the whole lake
# ... explore in the S3, EMR Serverless, and Athena consoles ...
AUTO_YES=1 bash deploy/99_teardown.sh   # remove everything, stop charges
```

That is it. You built a real data lake on AWS.

All glory to God! ✝️❤️
