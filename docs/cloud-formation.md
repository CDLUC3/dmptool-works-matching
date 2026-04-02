# CloudFormation Infrastructure

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

| What                  | Source stack                                 | Consumer        |
|-----------------------|----------------------------------------------|-----------------|
| VPC ID                | Organization VPC stack                       | batch-platform  |
| Subnet ID             | Organization subnet stack                    | batch-platform  |
| Security Group ID     | dmptool-infrastructure security-groups       | batch-platform  |
| OpenSearch Domain ARN | dmptool-infrastructure opensearch-domain     | batch-jobs      |
| RDS User Secret ARN   | dmptool-infrastructure rds                   | batch-jobs      |
