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
Create a `.env.aws-batch` file based on `.env.aws-batch.example`, and populate
all required AWS, S3, and job configuration variables.

### 1.3. Build and Push Image
Build the Docker image and push it to Amazon ECR:
```bash
docker build -t dmpworks:x86 -f Dockerfile.aws .
docker tag dmpworks:x86 <AWS_ACCOUNT_ID>.dkr.ecr.<AWS_REGION>.amazonaws.com/<ECR_REPOSITORY>:dmpworks-x86
docker push <AWS_ACCOUNT_ID>.dkr.ecr.<AWS_REGION>.amazonaws.com/<ECR_REPOSITORY>:dmpworks-x86
```

### 1.4. Upload DMPs
Begin by uploading the DMP export file
`coki-dmps_YYYY-MM-DD_1.jsonl.gz` to:

`s3://<BUCKET_NAME>/dmps/YYYY-MM-DD/download/coki-dmps_YYYY-MM-DD_1.jsonl.gz`

`YYYY-MM-DD` should match the snapshot date for this run. This is the only 
dataset that must be uploaded manually.

### 1.5. Upload Files for Dataset Subsets
If you want to work with subsets of the source datasets and limit DMP–work
searches to specific DMPs, create the files below and upload them to the
meta/ directory in your S3 bucket.

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

### 1.6. Running Partial Jobs
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
│ *  ENV --env                    Environment (e.g., dev, stage, prod) [choices: dev, stage, prod] [env var: ENV]   │
│                                 [required]                                                                        │
│ *  RUN-ID --run-id              A unique ID to represent this run of the job. [env var: ROR_RUN_ID] [required]    │
│ *  BUCKET-NAME --bucket-name    S3 bucket name for job I/O. [env var: BUCKET_NAME] [required]                     │
│ *  DOWNLOAD-URL --download-url  The Zenodo download URL for the ROR data file. [env var: ROR_DOWNLOAD_URL]        │
│                                 [required]                                                                        │
│ *  HASH --hash                  The expected hash of the data file. [env var: ROR_HASH] [required]                │
│ *  FILE-NAME --file-name        The name of the file to be transformed. [env var: ROR_FILE_NAME] [required]       │
│    START-JOB --start-job        The first job to run in the sequence. [choices: download, transform] [env var:    │
│                                 ROR_START_JOB] [default: download]                                                │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

## 2. Download and Normalise Datasets
Download and normalise ROR data:
```bash
dmpworks batch-submit ror
```

Download and normalise OpenAlex Works:
```bash
dmpworks batch-submit openalex-works
```

Download and normalise Crossref Metadata:
```bash
dmpworks batch-submit crossref-metadata
```

Download and normalise DataCite data:
```bash
dmpworks batch-submit datacite
```

## 3. Process Works
Next, works are processed by transforming the normalised source datasets into
the works index using SQLMesh (`sqlmesh-transform` job), and then syncing the
results to OpenSearch (`sync-works` job).

Run the following command to submit these AWS Batch jobs:
```bash
dmpworks batch-submit process-works
```

## 4. Process DMPs
Lastly, DMPs are processed by syncing them to OpenSearch (`sync-dmps` job),
enriching them with additional metadata (`enrich-dmps` job), running the DMP–works
search (`dmp-works-search` job), and merging published output matches into the
DMP Tool database (`merge-related-works` job).

Run the following command to submit these AWS Batch jobs:
```bash
dmpworks batch-submit process-dmps
```

This updates the OpenSearch DMP index and writes matched outputs back to the 
DMP Tool database.