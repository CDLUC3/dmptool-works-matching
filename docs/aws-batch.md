# Submitting AWS Batch Jobs

This guide describes how to build the DMP works matching Docker image and submit
AWS Batch jobs to download, normalise, and process DMP and works data.

## 1. Setup

Follow these steps to set up the environment variables and build and push the
DMP works Docker image. This guide assumes that the AWS Batch infrastructure
has already been deployed.

### 1.1. AWS Credentials

Install docker-credential-helper-ecr:

```bash
# On Linux:
sudo apt install amazon-ecr-credential-helper

# or on Mac
brew install docker-credential-helper-ecr
```

Then add the following `credHelpers` entry to `~/.docker/config.json`
(customise the AWS account ID and region). If the file already exists, merge
this into the existing JSON rather than replacing it.

```json
{
  "credHelpers": {
    "<AWS_ACCOUNT_ID>.dkr.ecr.<AWS_REGION>.amazonaws.com": "ecr-login"
  }
}
```

Then authenticate with AWS (for example, using SSO):

```bash
export AWS_PROFILE=my-profile
aws sso login
```

### 1.2. Environment Variables

Create a `.env.aws` file from `.env.aws.example` and populate all required AWS,
S3, and job configuration variables.

`dmpworks` does not load `.env.aws` automatically. Pass `--env-file .env.aws`
(or set `DMPWORKS_ENV_FILE=.env.aws`) when running any `batch-submit` command:

```bash
dmpworks --env-file .env.aws batch-submit <command>
# or
DMPWORKS_ENV_FILE=.env.aws dmpworks batch-submit <command>
```

### 1.3. Build and Push Image

Build the Docker image and push it to Amazon ECR:

```bash
docker build -t dmpworks:x86 -f Dockerfile.aws .
docker tag dmpworks:x86 <AWS_ACCOUNT_ID>.dkr.ecr.<AWS_REGION>.amazonaws.com/<ECR_REPOSITORY>:dmpworks-x86
docker push <AWS_ACCOUNT_ID>.dkr.ecr.<AWS_REGION>.amazonaws.com/<ECR_REPOSITORY>:dmpworks-x86
```

### 1.4. Upload Files

#### 1.4.1. Dataset Subsets

If you want to work with subsets of the source datasets and limit DMP–work
searches to specific DMPs, create the files below and upload them to the
`meta/` directory in your S3 bucket.

These files define which works, DMPs, and institutions are included during
subset processing.

`work_dois.json` — list of work DOIs to include in the dataset subset:

```json
[
  "10.0000/abc",
  "10.0000/def",
  "10.0000/ghi"
]
```

`institutions.json` — list of institutions whose affiliated works should be included:

```json
[
  {
    "name": "University of California, Berkeley",
    "ror": "01an7q238"
  }
]
```

`dmp_dois.json` — list of DMP DOIs to include when running DMP–work searches:

```json
[
  "10.48321/abc",
  "10.48321/def",
  "10.48321/ghi"
]
```

`dmp_institutions.json` — list of institutions whose DMPs should be included:

```json
[
  {
    "name": "University of California, Berkeley",
    "ror": "01an7q238"
  }
]
```

#### 1.4.2. Datasets

**Data Citation Corpus**

Extract the Data Citation Corpus JSON files from the ZIP archive, gzip them, and 
upload them to the `data_citation_corpus/2025-08-15/download/` directory in your S3 bucket:

```bash
for f in *.json; do gzip -k "$f"; done
s5cmd cp '*.json.gz' s3://<BUCKET_NAME>/data_citation_corpus/2025-08-15/download/
```

**DOI State**

Generate an empty DOI state file and upload it to S3. This file is used by SQLMesh as the initial DOI state:

```bash
dmpworks sqlmesh init-doi-state doi_state_00000.parquet
s5cmd cp doi_state_00000.parquet s3://<BUCKET_NAME>/sqlmesh/2025-01-01/doi_state_export/doi_state_00000.parquet
```

### 1.5. Running Partial Jobs

AWS Batch submit commands typically launch a sequence of dependent jobs that
run in order. If a job fails, you do not need to re-run the entire sequence.
Instead, you can resume from the failed job using the `--start-job` option.

To see which jobs are available, append `--help` and inspect the allowed values
for `--start-job`. For example, in the command below, `download` and `transform`
are valid options.

If the `download` job succeeds but `transform` fails, you can re-submit the job
and start from `transform`.

```bash
dmpworks batch-submit ror --help
Usage: dmpworks batch-submit ror [ARGS] [OPTIONS]

╭─ Parameters ──────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ *  ENV --env                    Environment (e.g., dev, stg, prd) [choices: dev, stg, prd] [env var: ENV]   │
│                                 [required]                                                                        │
│ *  RUN-ID --run-id              A unique ID to represent this run of the job. [env var: ROR_RUN_ID] [required]    │
│ *  BUCKET-NAME --bucket-name    S3 bucket name for job I/O. [env var: BUCKET_NAME] [required]                     │
│ *  DOWNLOAD-URL --download-url  The Zenodo download URL for the ROR data file. [env var: ROR_DOWNLOAD_URL]        │
│                                 [required]                                                                        │
│ *  HASH --hash                  The expected hash of the data file. [env var: ROR_HASH] [required]                │
│ *  FILE-NAME --file-name        The name of the file to be transformed. [env var: ROR_FILE_NAME] [required]       │
│    START-JOB --start-job        The first job to run in the sequence. [choices: download, transform] [env var:    │
│                                 ROR_START_JOB] [default: download]                                               │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

## 2. Download and Normalise Datasets

Each dataset is downloaded and normalised independently. The commands in this
section can be run in parallel — there is no ordering requirement between them.
Once all datasets have been normalised, proceed to [Section 3](#3-process-works).

### 2.1. ROR

Downloads ROR institution data, finds ROR JSON file in ZIP archive, extracts it,
gzips JSON file and uploads it to S3. This command runs a single step:

1. **`download`** — Download, extract, and upload ROR institution data to S3.

```bash
dmpworks --env-file .env.aws batch-submit ror
```

### 2.2. OpenAlex Works

Downloads and normalises OpenAlex Works data in three steps:

1. **`download`** — Downloads the OpenAlex Works snapshot from S3.
2. **`dataset-subset`** _(optional)_ — Filters the dataset to a subset of works
   or institutions (see [Section 1.4](#14-upload-files)).
3. **`transform`** — Normalises the downloaded data to Parquet.

```bash
dmpworks --env-file .env.aws batch-submit openalex-works
```

### 2.3. Crossref Metadata

Downloads and normalises Crossref Metadata in three steps:

1. **`download`** — Downloads the Crossref Metadata snapshot from S3.
2. **`dataset-subset`** _(optional)_ — Filters the dataset to a subset of works
   or institutions (see [Section 1.4](#14-upload-files)).
3. **`transform`** — Normalises the downloaded data to Parquet.

```bash
dmpworks --env-file .env.aws batch-submit crossref-metadata
```

### 2.4. DataCite

Downloads and normalises DataCite data in three steps:

1. **`download`** — Downloads the DataCite snapshot from S3.
2. **`dataset-subset`** _(optional)_ — Filters the dataset to a subset of works
   or institutions (see [Section 1.4](#14-upload-files)).
3. **`transform`** — Normalises the downloaded data to Parquet.

```bash
dmpworks --env-file .env.aws batch-submit datacite
```

## 3. Process Works

> **Prerequisite:** All datasets in [Section 2](#2-download-and-normalise-datasets)
> must be fully downloaded and normalised before running this step.

Transforms the normalised source datasets into the works index and syncs the
results to OpenSearch in two steps:

1. **`sqlmesh-transform`** — Transforms the normalised Parquet data into the
   works index using SQLMesh.
2. **`sync-works`** — Syncs the works index to OpenSearch.

```bash
dmpworks --env-file .env.aws batch-submit process-works
```

## 4. Process DMPs

> **Prerequisite:** [Section 3: Process Works](#3-process-works) must have completed
> and the works index must be loaded into OpenSearch before running this step.

Lastly, DMPs are processed in four steps:

1. **`sync-dmps`** — Fetches the latest DMP metadata from the DMP Tool MySQL
   database and synchronises it with the `dmps-index` OpenSearch index.
2. **`enrich-dmps`** — Enriches DMPs in the index with additional metadata.
3. **`dmp-works-search`** — Runs the DMP–works search to find matching outputs.
4. **`merge-related-works`** — Merges published output matches back into the
   DMP Tool database.

Run the following command to submit these AWS Batch jobs:

```bash
dmpworks --env-file .env.aws batch-submit process-dmps
```
