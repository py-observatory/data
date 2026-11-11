# pypi-downloads

How many times each PyPI project was downloaded on each UTC day, counted from PyPI's public BigQuery table, `bigquery-public-data.pypi.file_downloads`. Like the other streams, it publishes SQLite files compressed with zstd, to the `pypi-downloads-current` release and the monthly releases beside it. They appear when its first run, a bootstrap, has published (see "On GitHub"). The [repository README](../../README.md) says how to read them.

## What is published

Counting a day means reading the whole of its partition, which is too much for a runner. So the work is done by a Cloud Run job, the counter (`counter/`), that reads one day through the BigQuery Storage Read API and hands the day over as a small file in a bucket. The stream starts the counter, takes the file, and publishes it.

- `pypi-downloads-current` holds the state (`pypi-downloads-state.db.zst`), the open delta (`pypi-downloads-delta-open.db.zst`) and `manifest.json`.
- `pypi-downloads-month-YYYY-MM` holds the state as the month began, the delta of each day that had a run, and once the month is over, the month's deltas merged into one database and the month's manifest. Its assets are never replaced.

Every counted day, whether it was due or part of the backfill, goes into the run's open delta, so each day is published in the month in which it was counted. Each row carries its `day`, so a reader loses nothing.

Days start on 2016-01-22, the first in the table. Day D is counted on D+2 UTC or later. Two breaks in the series are recorded in the `breaks` table: days before 2018-07-26 are undercounted, and from 2026-08-24 PyPI no longer counts PEP 658 metadata fetches, which brought counts down by 35 to 37%.

A download made by a mirror client is counted separately. The mirror clients are `bandersnatch`, `z3c.pypimirror`, `Artifactory` and `devpi`, matched exactly against `details.installer.name`. The stream stores `mirror_downloads`, and downloads without mirrors is `downloads - mirror_downloads`.

### The tables

The history class of each table is in `stream.toml`.

- `meta` (state, snapshot): `applied_seq`, `bootstrap_at` and `launch` (the first day that the stream counted, as YYYY-MM-DD), each as a key and its value.
- `projects` (state, append): `project_id` and the canonical `name`. A project's first day with a download is the earliest `day` of its rows in `counts`. It is not stored here, because the backfill walks backward from launch and the table is append-only, so the day that first named a project is seldom its first.
- `days` (state, append): one row for each counted day, with `day` (days since 1970-01-01), `rows_bigquery`, `rows_counted`, `partition_modified`, `projects`, `downloads`, `mirror_downloads`, `null_project_rows`, `read_bytes`, the `execution` that counted it (null when the day came from the bucket), its `vcpu_seconds` and `gib_seconds`, `counted_at` and `seq`.
- `breaks` (state, append): a day on which the series breaks, and a note on why.
- `spend` (state, snapshot): for each billing month, the cost and the budget that the newest budget notification reported, and when it was sent.
- `clearances` (state, snapshot): for each billing month whose spend a person has cleared, the `cost` that was cleared, the `reason` they gave and `cleared_at` (see "Clearing a spend hold").
- `counts` (delta only, append): `project_id`, `day`, `downloads` and `mirror_downloads`, keyed by the project and the day. The state holds no counts: they are in the deltas and in the months' merged databases. A day counted twice in one month would break the merge loudly, and so cannot be counted twice without notice.

## The counts file

For each day D the counter writes two objects to the bucket `py-observatory-counts`, the database first.

- `counts/D.db.zst` is a SQLite database compressed with zstd at level 19. Its `counts` table has one row for each project, with `downloads` above 0 and `mirror_downloads` between 0 and `downloads`. Its `meta` table holds the day, how many rows were counted and how many BigQuery had, when the partition was last modified, the bytes read, how many rows had no project, and the times, streams and workers of the counter's run.
- `counts/D.json` holds `day`, `db_bytes` and `db_sha256` (of the compressed object), `rows_counted`, `rows_bigquery`, `partition_modified`, `read_bytes` and `null_project_rows`. It is written last, so it marks a finished day.

The stream checks a counts file before it writes any row of it: it must be for the day that was asked for, its checksum and size must match, its totals must add up, and each name and count must be in range. A name that is not canonical is stored in its canonical form, and names that come to the same canonical name are merged, with their counts summed. The report's `renamed` counts the names that were not canonical. The bucket deletes `counts/` objects after 14 days. They are handover copies of what the stream publishes, and never a published asset.

A counter execution can outlive the run that started it. The next run finds the day's files in the bucket, takes them and does not start the job again.

### A bad counts file

A run reuses `counts/YYYY-MM-DD.json` and `counts/YYYY-MM-DD.db.zst` when the summary exists. A file that fails its checks fails that day on every run, and every later day waits behind it, until both objects are deleted or the bucket's 14-day lifecycle removes them. Delete them, and the next run counts the day again:

```
gcloud storage rm gs://py-observatory-counts/counts/YYYY-MM-DD.*
```

## The budget guard and the free tier

The stream is meant to cost nothing, always. Google's free tier is counted for each billing account each month, and a month is a billing month, which follows America/Los_Angeles and not UTC. For the parts of it that the stream uses, the free tier is:

- Cloud Run jobs: 240,000 vCPU-seconds and 450,000 GiB-seconds.
- BigQuery Storage Read API: 300 TiB of reads.
- BigQuery SQL: 1 TiB. The stream's only query is on `INFORMATION_SCHEMA.PARTITIONS`, which scans no table data.
- Cloud Storage in us-central1: 5 GB-months, and 100 GB a month outbound from North America. Nothing large is read from outside Google Cloud.

Before it starts each day, the run asks the guard, which holds the run in these cases:

- The budget has reported a spend above $0, in this billing month or an earlier one, that no clearance covers. This stops all counting, and the hold does not lapse when a new billing month begins: it lasts until a person has looked (see "Clearing a spend hold").
- Any of the three measured parts has reached 98% of its free tier, or the next execution's worst case would take it there. This stops all counting.
- For the backfill only: some measure is above `0.8 × free × elapsed`, where `elapsed` is the share of the billing month that has passed, or no budget notification for the month that is less than 48 hours old has been seen. So the backfill is spread evenly over the month, daily counting always has room, and no news is no backfill.

The usage is taken from Cloud Run itself: the vCPU- and GiB-seconds of every execution of the job that started this billing month, which includes executions of runs that died. The bytes read are summed from the days counted this month. Each Read API session is limited to one day's partition.

The budget counts the free tier's credit and no other: Cloud Run's free tier arrives as a credit, so a budget that excludes every credit reports free use as spend, and the stream would hold. Promotions stay excluded, so that a trial's credit cannot hide a real charge. The setup under "Google Cloud" sets this.

The budget's notifications come from a Pub/Sub subscription, `budget-alerts-pull`, which each run reads. The newest one of the month is kept in the `spend` table, and a message is acknowledged only once its spend is published.

A run also reads the partition row counts of the newest 10 days after launch. A day whose count has moved since it was counted is named in the report as drift. It is detected, not recounted. When that read fails, the report says `drift_unknown: true`, and the run goes on to count and publish.

### Clearing a spend hold

A spend above $0 holds every day until a person has looked at it. Look at the billing report first (Billing, then Reports, for the project `py-observatory`), and find what was charged and why. Then dispatch `tick.yml` with the stream named in `override_for` and what you found in `override_reason`:

```
gh workflow run tick.yml -f override_for=pypi-downloads -f override_reason="..."
```

The run records a clearance for each billing month whose spend holds it, with the month's cost as its newest notification reports it and the reason, publishes it in the `clearances` table, and counts. A clearance covers only the cost it records: if a later notification shows more spent in that month, the stream holds again until it is looked at and cleared again. The same reason also overrides any publish gate that fails in that run.

## Google Cloud

The project is `py-observatory` (number 893320255656), the region `us-central1` and the bucket `py-observatory-counts`. Nothing here is a secret: the names, the numbers and the repository id are public. The tick signs in as the service account `downloads-github`, and the deploy workflow as `counter-deployer`, both by Workload Identity Federation with GitHub's OIDC token and no key. The provider admits only this repository (id 1441905416), on `refs/heads/main`, in the environment `gcp`. The counter itself runs as `counter-runtime`.

These are the commands, in order, that build it, so that it can be rebuilt. They need an owner's sign-in (`gcloud auth login`). Already in place before them are the services BigQuery, BigQuery Storage, Cloud Run, IAM, IAM Credentials, STS, Pub/Sub, Resource Manager, Cloud Billing and Billing Budgets, the bucket `py-observatory-counts` (us-central1, uniform access, public access prevention, no soft delete), and the budget's Pub/Sub topic `budget-alerts` with its pull subscription `budget-alerts-pull`.

```sh
P=py-observatory
N=893320255656
R=us-central1
B=py-observatory-counts
REPO_ID=1441905416
SA_SUFFIX=$P.iam.gserviceaccount.com

# The image registry, which keeps the two newest images and deletes the rest.
gcloud services enable artifactregistry.googleapis.com --project=$P
gcloud artifacts repositories create counter --repository-format=docker \
  --location=$R --project=$P --description="The downloads counter's image"
cat > cleanup.json <<'JSON'
[{"name": "keep-two", "action": {"type": "Keep"}, "mostRecentVersions": {"keepCount": 2}},
 {"name": "delete-the-rest", "action": {"type": "Delete"}, "condition": {"tagState": "ANY"}}]
JSON
gcloud artifacts repositories set-cleanup-policies counter --location=$R --project=$P \
  --policy=cleanup.json --no-dry-run

# The counts the counter hands over are deleted 14 days after they are written.
cat > lifecycle.json <<'JSON'
{"rule": [{"action": {"type": "Delete"}, "condition": {"age": 14, "matchesPrefix": ["counts/"]}}]}
JSON
gcloud storage buckets update gs://$B --lifecycle-file=lifecycle.json

# Three service accounts: the counter runs as counter-runtime, the tick signs in as
# downloads-github, and the deploy workflow as counter-deployer.
for sa in counter-runtime downloads-github counter-deployer; do
  gcloud iam service-accounts create $sa --project=$P
done

# counter-runtime: read sessions and the metadata query, and writes to the bucket.
gcloud projects add-iam-policy-binding $P --condition=None \
  --member=serviceAccount:counter-runtime@$SA_SUFFIX --role=roles/bigquery.readSessionUser
gcloud projects add-iam-policy-binding $P --condition=None \
  --member=serviceAccount:counter-runtime@$SA_SUFFIX --role=roles/bigquery.jobUser
gcloud storage buckets add-iam-policy-binding gs://$B \
  --member=serviceAccount:counter-runtime@$SA_SUFFIX --role=roles/storage.objectAdmin

# downloads-github: runs the job and watches it, reads the bucket, pulls the budget's
# notifications, and runs the drift query.
gcloud projects add-iam-policy-binding $P --condition=None \
  --member=serviceAccount:downloads-github@$SA_SUFFIX --role=roles/run.viewer
gcloud projects add-iam-policy-binding $P --condition=None \
  --member=serviceAccount:downloads-github@$SA_SUFFIX --role=roles/run.jobsExecutorWithOverrides
gcloud projects add-iam-policy-binding $P --condition=None \
  --member=serviceAccount:downloads-github@$SA_SUFFIX --role=roles/bigquery.jobUser
gcloud storage buckets add-iam-policy-binding gs://$B \
  --member=serviceAccount:downloads-github@$SA_SUFFIX --role=roles/storage.objectViewer
gcloud pubsub subscriptions add-iam-policy-binding budget-alerts-pull --project=$P \
  --member=serviceAccount:downloads-github@$SA_SUFFIX --role=roles/pubsub.subscriber

# counter-deployer: deploys the job to run as counter-runtime, and pushes images.
gcloud projects add-iam-policy-binding $P --condition=None \
  --member=serviceAccount:counter-deployer@$SA_SUFFIX --role=roles/run.developer
gcloud iam service-accounts add-iam-policy-binding counter-runtime@$SA_SUFFIX --project=$P \
  --member=serviceAccount:counter-deployer@$SA_SUFFIX --role=roles/iam.serviceAccountUser
gcloud artifacts repositories add-iam-policy-binding counter --location=$R --project=$P \
  --member=serviceAccount:counter-deployer@$SA_SUFFIX --role=roles/artifactregistry.writer

# The budget counts what is owed: Cloud Run's free tier arrives as a FREE_TIER credit, so
# a budget that excludes every credit reports free use as spend and the stream would hold
# for good. Promotions (a trial's credit) stay excluded, so they cannot hide a real charge.
ACCOUNT=$(gcloud billing projects describe $P --format='value(billingAccountName)')
ACCOUNT=${ACCOUNT#billingAccounts/}
BUDGET=$(gcloud billing budgets list --billing-account=$ACCOUNT \
  --billing-project=$P --format='value(name)')
gcloud billing budgets update "$BUDGET" --billing-account=$ACCOUNT \
  --billing-project=$P --credit-types-treatment=include-specified-credits \
  --filter-credit-types=FREE_TIER

# Workload Identity Federation: GitHub's OIDC tokens from this repository, on main, in the
# gcp environment, and nothing else.
gcloud iam workload-identity-pools create github --location=global --project=$P \
  --display-name="GitHub Actions"
gcloud iam workload-identity-pools providers create-oidc data --location=global \
  --workload-identity-pool=github --project=$P \
  --issuer-uri=https://token.actions.githubusercontent.com \
  --attribute-mapping=google.subject=assertion.sub,attribute.repository_id=assertion.repository_id,attribute.ref=assertion.ref,attribute.environment=assertion.environment \
  --attribute-condition="assertion.repository_id == '$REPO_ID' && assertion.ref == 'refs/heads/main' && assertion.environment == 'gcp'"
for sa in downloads-github counter-deployer; do
  gcloud iam service-accounts add-iam-policy-binding $sa@$SA_SUFFIX --project=$P \
    --role=roles/iam.workloadIdentityUser \
    --member="principalSet://iam.googleapis.com/projects/$N/locations/global/workloadIdentityPools/github/attribute.repository_id/$REPO_ID"
done
```

On GitHub, the environment `gcp` allows the branch `main` only:

```sh
gh api -X PUT repos/py-observatory/data/environments/gcp \
  -F 'deployment_branch_policy[protected_branches]=false' \
  -F 'deployment_branch_policy[custom_branch_policies]=true'
gh api -X POST repos/py-observatory/data/environments/gcp/deployment-branch-policies \
  -f name=main -f type=branch
```

The Cloud Run job `counter` is made by the first run of `deploy-counter.yml`, from `stream.toml`'s `[counter]` values.

## Running

A run publishes to `GITHUB_REPOSITORY` (`py-observatory/data` when it is not set), with the token in `GITHUB_TOKEN`, and signs in to Google Cloud through the OIDC token that the workflow gives it:

```
uv run --package pypi-downloads python -m pypi_downloads run
```

`--bootstrap` counts the newest day that is due as the launch day and publishes the first state, and only a stream that has never published accepts it. `--override-reason TEXT` publishes although a publish gate failed, and the manifest logs the reason. It also clears a spend that holds the run (see "Clearing a spend hold"). `--max-seconds` is how long after its start a run stops starting days (3,600 by default, and 3,000 in the tick: see below). `--workdir PATH` is the folder in which a run makes its own directory and removes it afterwards (`work` by default).

`python -m pypi_downloads deploy --image IMAGE` creates or updates the counter's job to run `IMAGE`. The deploy workflow calls it.

The run prints its report as JSON.

### Exit 2

The run exits with 2, after it has published, when:

- the guard held it for a spend above $0 that no clearance covers;
- the guard held it at the 98% stop, which is usage plus the next execution's worst case;
- the guard held the backfill for a missing budget notification, and no notification of any month has come in the last 48 hours, so the feed looks broken;
- a day failed;
- drift was found: a counted day whose partition row count has moved; or
- the drift is unknown, because the read of the partition row counts failed.

A hold at the budget line, where the backfill has used its even share of the month, is normal and exits with 0. So is a backfill held at the start of a billing month whose first notification has not come yet, while the last month's still do. The report names either hold.

### On GitHub

`tick.yml` runs the stream every six hours, in its job `pypi-downloads`, in the environment `gcp` and with `id-token: write`, which no other job of the tick has. It has a concurrency group of its own. Its run stops starting days after 3,000 s. That only decides whether a day starts: a day started just before it can wait for the counter's task limit and a margin (1,800 + 300 = 2,100 s) and one 30 s poll, and publishing takes a few minutes, so a run lasts about 90 minutes at most, inside the 97 minutes of its step and the 100 of its job. The catalog does not wait for it. Its failures keep one rolling issue, `[pypi-downloads] scheduled run failed`, which gets a comment for each run that fails again and is closed by the first run that succeeds.

`deploy-counter.yml` builds the counter's image on the runner, pushes it to Artifact Registry as `us-central1-docker.pkg.dev/py-observatory/counter/counter:<commit>`, and deploys the Cloud Run job. It runs when the counter, `stream.toml` or `run.py` (whose `counter_template` makes the job from `stream.toml`'s `[counter]` values) changes on `main`, and when it is dispatched. Deploys run one at a time, and one that has begun is never cancelled. The deployer's access token is written to a file, masked and removed afterwards, and never reaches the log.

The first steps, in order:

1. Run the commands of "Google Cloud" above.
2. Deploy the counter, which makes the Cloud Run job:

   ```
   gh workflow run deploy-counter.yml
   ```

3. Bootstrap the stream, with a dispatch of `tick.yml` with `bootstrap` set to `pypi-downloads`:

   ```
   gh workflow run tick.yml -f bootstrap=pypi-downloads
   ```

The other jobs of the tick run in the same dispatch as ordinary runs. A scheduled run that comes before the bootstrap fails closed and opens the stream's issue. That is expected, and the first run that succeeds closes it.

An override is a dispatch of `tick.yml` that names the stream in `override_for` and gives the reason in `override_reason`.

## In this folder

- `stream.toml`: the stream's id, its upstream, each table with its history class, and the `[gcp]` and `[counter]` values that the run, the deploy and the workflows read.
- `src/pypi_downloads/`: the stream. `plan.py` says which days are due and holds the budget guard, `counts.py` reads and checks a counts file, `store.py` holds the databases, and `run.py` is the run and its command line.
- `counter/`: the Cloud Run job, with its own `pyproject.toml`, `uv.lock` and `Dockerfile`. It is not a member of the workspace, and it never imports the stream's code or the kit.
