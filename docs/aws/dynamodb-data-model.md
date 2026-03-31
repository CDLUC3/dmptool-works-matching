# DynamoDB Data Model

The pipeline uses five DynamoDB tables to track dataset releases, task execution
state, and pipeline run records.

Models are defined in
[dynamodb_store.py](../../python/dmpworks/scheduler/dynamodb_store.py)
using [PynamoDB](https://pynamodb.readthedocs.io/).

## 1. Dataset Releases

**Table:** `dmpworks-{env}-dataset-releases`

Tracks discovered dataset releases and their lifecycle through the ingest
pipeline. One record per dataset version.

| Attribute                     | Type   | Key   | Description                                                           |
|-------------------------------|--------|-------|-----------------------------------------------------------------------|
| `dataset`                     | String | PK    | Dataset identifier (e.g. `openalex-works`, `ror`)                     |
| `release_date`                | String | SK    | Release date `YYYY-MM-DD`                                             |
| `status`                      | String |       | DISCOVERED, STARTED, COMPLETED, FAILED, ABORTED, WAITING_FOR_APPROVAL |
| `download_url`                | String |       | Direct download URL                                                   |
| `file_name`                   | String |       | Filename to download                                                  |
| `file_hash`                   | String |       | MD5 checksum                                                          |
| `metadata`                    | Map    |       | Extra key/value pairs                                                 |
| `step_function_execution_arn` | String |       | ARN of the SFN execution processing this release                      |
| `approval_token`              | String |       | Task token for retry approval gate                                    |
| `approval_task_name`          | String |       | Child task awaiting retry approval                                    |
| `created_at`                  | String |       | ISO datetime                                                          |
| `updated_at`                  | String |       | ISO datetime                                                          |

## 2. Task Runs

**Table:** `dmpworks-{env}-task-runs`

Records individual Batch task executions (download, transform, sqlmesh, etc.)
across all pipeline types.

| Attribute                     | Type    | Key   | Description                                                       |
|-------------------------------|---------|-------|-------------------------------------------------------------------|
| `run_name`                    | String  | PK    | Job name, e.g. `openalex-works-download`, `process-works-sqlmesh` |
| `run_id`                      | String  | SK    | Unique execution ID                                               |
| `status`                      | String  |       | STARTED, COMPLETED, FAILED, ABORTED                               |
| `step_function_execution_arn` | String  | GSI   | ARN of the enclosing SFN execution                                |
| `error`                       | String  |       | Error message if failed                                           |
| `metadata`                    | Map     |       | Context (dataset, release_date, etc.)                             |
| `cleanup_scheduled`           | Boolean |       | Whether S3 cleanup has been scheduled                             |
| `created_at`                  | String  |       | ISO datetime                                                      |
| `updated_at`                  | String  |       | ISO datetime                                                      |

**GSI:** `step-function-execution-arn-index` (hash: `step_function_execution_arn`,
projection: KEYS_ONLY). Used by the failure handler to find task runs by SFN
execution ARN.

## 3. Task Checkpoints

**Table:** `dmpworks-{env}-task-checkpoints`

Marks the last completed run for each workflow task. Used by state machines
to skip already-completed steps on re-run.

| Attribute           | Type    | Key   | Description                                                  |
|---------------------|---------|-------|--------------------------------------------------------------|
| `workflow_key`      | String  | PK    | Workflow identifier (e.g. `openalex-works`, `process-works`) |
| `task_key`          | String  | SK    | Composite key: `{task_name}#{YYYY-MM-DD}`                    |
| `run_id`            | String  |       | Run ID of the completed execution                            |
| `cleanup_scheduled` | Boolean |       | Whether S3 cleanup has been scheduled                        |
| `completed_at`      | String  |       | ISO datetime                                                 |

The composite sort key enables both exact lookups (`workflow_key` +
`task_name#date`) and queries for the latest checkpoint per task (reverse scan
on `task_key` with a begins-with prefix).

## 4. Process Works Runs

**Table:** `dmpworks-{env}-process-works-runs`

Tracks each run of the monthly process-works pipeline (SQLMesh + OpenSearch sync).
Records which dataset run IDs and release dates were used as inputs.

| Attribute                           | Type   | Key   | Description                                               |
|-------------------------------------|--------|-------|-----------------------------------------------------------|
| `release_date`                      | String | PK    | Monthly run date `YYYY-MM-DD`                             |
| `run_id`                            | String | SK    | Unique execution ID                                       |
| `status`                            | String |       | STARTED, COMPLETED, FAILED, ABORTED, WAITING_FOR_APPROVAL |
| `step_function_execution_arn`       | String |       | ARN of the SFN execution                                  |
| `run_id_sqlmesh_prev`               | String |       | Prior SQLMesh run ID (for incremental runs)               |
| `run_id_openalex_works`             | String |       | OpenAlex Works transform checkpoint run ID                |
| `run_id_datacite`                   | String |       | DataCite transform checkpoint run ID                      |
| `run_id_crossref_metadata`          | String |       | Crossref Metadata transform checkpoint run ID             |
| `run_id_ror`                        | String |       | ROR download checkpoint run ID                            |
| `run_id_data_citation_corpus`       | String |       | Data Citation Corpus download checkpoint run ID           |
| `run_id_sqlmesh`                    | String |       | SQLMesh run ID (set after completion)                     |
| `release_date_openalex_works`       | String |       | Release date of the OpenAlex dataset used                 |
| `release_date_datacite`             | String |       | Release date of the DataCite dataset used                 |
| `release_date_crossref_metadata`    | String |       | Release date of the Crossref dataset used                 |
| `release_date_ror`                  | String |       | Release date of the ROR dataset used                      |
| `release_date_data_citation_corpus` | String |       | Release date of the Data Citation Corpus used             |
| `approval_token`                    | String |       | Task token for retry approval gate                        |
| `approval_task_name`                | String |       | Child task awaiting retry approval                        |
| `error`                             | String |       | Error message if failed                                   |
| `created_at`                        | String |       | ISO datetime                                              |
| `updated_at`                        | String |       | ISO datetime                                              |

## 5. Process DMPs Runs

**Table:** `dmpworks-{env}-process-dmps-runs`

Tracks each run of the process-dmps pipeline (sync, enrich, search, merge).
Task run IDs are recorded as each stage finishes.

| Attribute                     | Type   | Key  | Description                                               |
|-------------------------------|--------|------|-----------------------------------------------------------|
| `release_date`                | String | PK   | Daily run date `YYYY-MM-DD`                               |
| `run_id`                      | String | SK   | Unique execution ID                                       |
| `status`                      | String |      | STARTED, COMPLETED, FAILED, ABORTED, WAITING_FOR_APPROVAL |
| `step_function_execution_arn` | String |      | ARN of the SFN execution                                  |
| `run_id_sync_dmps`            | String |      | Sync DMPs task run ID                                     |
| `run_id_enrich_dmps`          | String |      | Enrich DMPs task run ID                                   |
| `run_id_dmp_works_search`     | String |      | DMP Works Search task run ID                              |
| `run_id_merge_related_works`  | String |      | Merge Related Works task run ID                           |
| `approval_token`              | String |      | Task token for retry approval gate                        |
| `approval_task_name`          | String |      | Child task awaiting retry approval                        |
| `error`                       | String |      | Error message if failed                                   |
| `created_at`                  | String |      | ISO datetime                                              |
| `updated_at`                  | String |      | ISO datetime                                              |

## Relationships

The version checker discovers **dataset releases**, which drive the ingest
pipeline. Each ingest step creates a **task run** record and, on success,
a **task checkpoint**. If a checkpoint already exists for a given date, the
state machine skips that step on re-run.

**Process-works runs** are created once all 5 dataset checkpoints exist.
Each record stores the `run_id` and `release_date` of every input dataset.

**Process-dmps runs** record the `run_id` of each of their 4 stages on
completion.

Three tables use an **approval token** pattern (dataset-releases,
process-works-runs, process-dmps-runs): when a child task fails, the parent
state machine stores a token and waits up to 7 days for retry via
`dmpworks pipeline runs approve-retry`.
