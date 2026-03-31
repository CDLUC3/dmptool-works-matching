# AWS Architecture

- [Cross-project stack dependencies](#cross-project-stack-dependencies)
- [Runtime resource flow](#runtime-resource-flow)
- [External dependencies](#external-dependencies)
- [EventBridge Schedules](#eventbridge-schedules)
- [State Machines](#state-machines)
  - [Dataset Ingest](#dataset-ingest)
  - [Process Works](#process-works)
  - [Process DMPs](#process-dmps)

Infrastructure is deployed via [Sceptre](https://docs.sceptre-project.org/)
from `infra/`. This project's stacks depend on shared infrastructure managed in
a separate Sceptre project (`dmptool-infrastructure`) via `!stack_output_external`
references. The dependency is one-way — dmptool-infrastructure has no references
back to this project.

## Cross-project stack dependencies

```mermaid
flowchart LR
    subgraph vpc ["Organization VPC"]
        VPC["VPC + Subnets"]
    end

    subgraph infra ["dmptool-infrastructure"]
        SG["security-groups"]
        OS["opensearch-domain"]
        RDS["rds"]
        CLUSTER["ecs/cluster"]
        APOLLO["ecs/apollo"]
        PROXY["ecs/opensearch-proxy"]
    end

    subgraph works ["dmptool-works-matching"]
        BPLAT["batch-platform"]
        BJOBS["batch-jobs"]
        S3["s3"]
        ECR["ecr"]
        DYNAMO["dynamodb"]
        SCHED["scheduler"]
    end

    VPC --> SG
    VPC --> CLUSTER
    VPC --> BPLAT

    SG -- "SecurityGroupId" --> BPLAT
    OS -- "OpenSearchDomainArn" --> BJOBS
    RDS -- "AWSBatchRDSUserSecretArn" --> BJOBS

    S3 --> BJOBS
    S3 --> SCHED
```

## Runtime resource flow

```mermaid
flowchart TD
    subgraph triggers ["EventBridge"]
        CRON["Scheduled rules\n(4 cron + 1 event)"]
    end

    subgraph compute ["Scheduler (Lambda + Step Functions)"]
        LAMBDA["Lambda functions (13)"]
        SFN["Step Functions (3)"]
    end

    subgraph batch ["AWS Batch"]
        JOBDEFS["Job definitions (3)\nstandard · database · datacite"]
        QUEUES["Job queues (5)\nsmall · download · transform\nsqlmesh · opensearch"]
        CE["Compute environments (5)\nEC2 — on demand, scale to zero"]
        CONTAINERS["Job containers"]

        QUEUES --> CE --> CONTAINERS
        JOBDEFS -. "image + config" .-> CONTAINERS
    end

    subgraph storage ["Storage"]
        S3[("S3 bucket")]
        DYNAMO[("DynamoDB\n5 tables")]
    end

    subgraph external ["External (dmptool-infrastructure)"]
        OS[("OpenSearch")]
        RDS[("RDS MySQL")]
    end

    ECR["ECR (container images)"]

    CRON --> LAMBDA
    LAMBDA <--> DYNAMO
    LAMBDA <--> SFN
    SFN -- "SubmitJob" --> QUEUES
    CONTAINERS <--> S3
    CONTAINERS <--> OS
    CONTAINERS --> RDS
    ECR -.-> LAMBDA
    ECR -.-> CONTAINERS
```

## External dependencies

Resources imported from other CloudFormation stacks at deploy time via
`!stack_output_external` (configured in `infra/vars-{env}.yaml`):

| What                  | Source stack                             | Consumer       |
|-----------------------|------------------------------------------|----------------|
| VPC ID                | Organization VPC stack                   | batch-platform |
| Subnet ID             | Organization subnet stack                | batch-platform |
| Security Group ID     | dmptool-infrastructure security-groups   | batch-platform |
| OpenSearch Domain ARN | dmptool-infrastructure opensearch-domain | batch-jobs     |
| RDS User Secret ARN   | dmptool-infrastructure rds               | batch-jobs     |

## EventBridge Schedules

Four scheduled rules trigger the pipeline. Schedules can be managed with
[`dmpworks pipeline schedules`](pipeline.md#schedules).

| Schedule                   | Cron                       | Timing (PDT)                    | Triggers                                                               |
|----------------------------|----------------------------|---------------------------------|------------------------------------------------------------------------|
| `version-checker-schedule` | `cron(0 15 ? * MON-FRI *)` | Mon-Fri 08:00                   | Checks for new dataset releases; starts dataset-ingest per new release |
| `process-works-schedule`   | `cron(0 16 ? * 2#2 *)`     | 2nd Monday 09:00                | Starts the process-works pipeline                                      |
| `process-dmps-schedule`    | `cron(0 3 ? * TUE-SAT *)`  | Mon-Fri 20:00                   | Starts the process-dmps pipeline                                       |
| `s3-cleanup-schedule`      | `cron(0 0 L * ? *)`        | Last day of month 17:00         | Schedules stale S3 run data for lifecycle expiry                       |

An additional **ExecutionFailedRule** watches all state machines for FAILED or
ABORTED executions and routes them to a failure handler Lambda that updates the
corresponding DynamoDB run records.

## State Machines

The scheduler stack defines 3 parent state machines and 9 child state machines.
Each child follows the same pattern: build Batch job parameters, submit the job,
mark the task complete, and signal the parent. On failure, the parent enters an
approval gate that waits up to 7 days for manual retry via
[`dmpworks pipeline runs approve-retry`](pipeline.md#23-approve-retry).

All state machines use checkpoint-based skip logic — before invoking a child,
the parent checks DynamoDB for an existing task checkpoint. If one exists, the
step is skipped, so re-runs after partial failures only repeat the failed steps.

### Dataset Ingest

Started per-dataset by the version checker when a new release is discovered.

```mermaid
flowchart TD
    Start([Start]) --> MarkStarted["Mark release STARTED"]
    MarkStarted --> DLCheck{"Download\ncompleted?"}

    DLCheck -->|skip| HasTransform
    DLCheck -->|run| Download["Download\n(child SM → Batch)"]
    Download -->|success| HasTransform
    Download -->|failure| DLApproval["Wait for approval\n(up to 7 days)"]
    DLApproval -->|approved| Download

    HasTransform{"Has transform\nstep?"}
    HasTransform -->|"ror, data-citation-corpus"| Done["Mark release COMPLETED"]
    HasTransform -->|other datasets| SubsetCheck{"Use subset?"}

    SubsetCheck -->|yes| SSCheck{"Subset\ncompleted?"}
    SubsetCheck -->|no| TFCheck

    SSCheck -->|skip| TFCheck
    SSCheck -->|run| Subset["Subset\n(child SM → Batch)"]
    Subset -->|success| TFCheck{"Transform\ncompleted?"}
    Subset -->|failure| SSApproval["Wait for approval"]
    SSApproval -->|approved| Subset

    TFCheck -->|skip| Done
    TFCheck -->|run| Transform["Transform\n(child SM → Batch)"]
    Transform -->|success| Done
    Transform -->|failure| TFApproval["Wait for approval"]
    TFApproval -->|approved| Transform
```

Datasets: openalex-works, datacite, crossref-metadata, ror, data-citation-corpus.
ROR and Data Citation Corpus only need the download step.

### Process Works

Runs monthly. Waits for all 5 dataset ingests to complete, then builds the
unified works index and syncs it to OpenSearch.

```mermaid
flowchart TD
    Start([Start]) --> Poll{"All 5 datasets\nready?"}
    Poll -->|no| Wait["Wait 1 hour"] --> Poll
    Poll -->|"not ready after 1 week"| Fail([FAILED])
    Poll -->|yes| CreateRun["Create process-works run"]

    CreateRun --> SMCheck{"SQLMesh\ncompleted?"}
    SMCheck -->|skip| SWCheck
    SMCheck -->|run| SQLMesh["SQLMesh\n(child SM → Batch)"]
    SQLMesh -->|success| SWCheck{"Sync works\ncompleted?"}
    SQLMesh -->|failure| SMApproval["Wait for approval"]
    SMApproval -->|approved| SQLMesh

    SWCheck -->|skip| Done
    SWCheck -->|run| SyncWorks["Sync works\n(child SM → Batch)"]
    SyncWorks -->|success| Done["Mark COMPLETED"]
    SyncWorks -->|failure| SWApproval["Wait for approval"]
    SWApproval -->|approved| SyncWorks

    Done --> Chain{"Start\nprocess-dmps?"}
    Chain -->|yes| FireDMPs["Start process-dmps\n(async, does not wait)"]
    Chain -->|no| End([End])
    FireDMPs --> End
```

Required dataset checkpoints: openalex-works (transform), datacite (transform),
crossref-metadata (transform), ror (download), data-citation-corpus (download).

### Process DMPs

Runs daily on weekdays. Also started by process-works on completion
(async — process-works does not wait for it to finish).

```mermaid
flowchart TD
    Start([Start]) --> CreateRun["Create process-dmps run"]
    CreateRun --> SD{"Sync DMPs\ncompleted?"}

    SD -->|skip| ED
    SD -->|run| SyncDmps["Sync DMPs\n(child SM → Batch)"]
    SyncDmps -->|success| ED{"Enrich DMPs\ncompleted?"}
    SyncDmps -->|failure| SDApproval["Wait for approval"]
    SDApproval -->|approved| SyncDmps

    ED -->|skip| DWS
    ED -->|run| EnrichDmps["Enrich DMPs\n(child SM → Batch)"]
    EnrichDmps -->|success| DWS{"DMP works search\ncompleted?"}
    EnrichDmps -->|failure| EDApproval["Wait for approval"]
    EDApproval -->|approved| EnrichDmps

    DWS -->|skip| MRW
    DWS -->|run| Search["DMP works search\n(child SM → Batch)"]
    Search -->|success| MRW{"Merge related works\ncompleted?"}
    Search -->|failure| DWSApproval["Wait for approval"]
    DWSApproval -->|approved| Search

    MRW -->|skip| Done
    MRW -->|run| Merge["Merge related works\n(child SM → Batch)"]
    Merge -->|success| Done["Mark COMPLETED"]
    Merge -->|failure| MRWApproval["Wait for approval"]
    MRWApproval -->|approved| Merge
```

Steps: sync-dmps, enrich-dmps, dmp-works-search, merge-related-works.
