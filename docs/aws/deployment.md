# Deploying dmpworks to AWS

This guide covers deploying all dmpworks CloudFormation stacks and operating
them after deployment. It assumes AWS credentials are already configured
(see [Submitting AWS Batch Jobs](batch.md) for credential setup).

## Required Variables

Set these before running any commands in this guide:

| Variable       | Description                        | Example                                        |
|----------------|------------------------------------|------------------------------------------------|
| `ENV`          | Target environment                 | `dev` or `stg`                                 |
| `ECR_REGISTRY` | ECR registry hostname              | `123456789012.dkr.ecr.us-west-2.amazonaws.com` |
| `SSM_PREFIX`   | SSM path prefix for all parameters | `/your-org/your-app/dmpworks`                  |

```bash
export ENV=dev
export ECR_REGISTRY=123456789012.dkr.ecr.us-west-2.amazonaws.com
export SSM_PREFIX=/your-org/your-app/dmpworks
```

`ECR_REGISTRY` is required for any target that pushes Docker images (Section 4).
`SSM_PREFIX` is required for any target that reads or writes SSM parameters.
The Makefile will error if either is not set when needed.

## 1. SSM Parameters

These must exist before deploying. Create them once with `aws ssm put-parameter`
(requires `ENV` and `SSM_PREFIX` from [Required Variables](#required-variables)).

| Parameter                                              | Used by    | Description                                                                            |
|--------------------------------------------------------|------------|----------------------------------------------------------------------------------------|
| `$SSM_PREFIX/$ENV/CloudFormationBucket`                | all stacks | S3 bucket Sceptre uses to upload CloudFormation templates                              |
| `$SSM_PREFIX/$ENV/DataCiteCredentialsAPIUserSecretArn` | batch      | Secrets Manager ARN for DataCite API credentials                                       |
| `$SSM_PREFIX/$ENV/LambdaEcrImageUri`                   | scheduler  | ECR image digest URI (`repo@sha256:<digest>`); managed by `make push-lambda-{env}`     |
| `$SSM_PREFIX/$ENV/LambdaConfig`                        | scheduler  | YAML configuration for all Lambda functions (see [Section 2](#2-lambda-config-yaml))   |

```bash
aws ssm put-parameter --name "$SSM_PREFIX/$ENV/CloudFormationBucket"                --value "<CLOUDFORMATION_BUCKET_NAME>" --type String --overwrite
aws ssm put-parameter --name "$SSM_PREFIX/$ENV/DataCiteCredentialsAPIUserSecretArn" --value "<DATACITE_SECRET_ARN>"        --type String --overwrite
```

## 2. Lambda Config YAML

All Lambda function configuration (dataset sources, transform settings, search
settings, etc.) is stored as a YAML document in the `LambdaConfig` SSM parameter.
Maintain a separate file per environment so dev and stg can diverge independently.

Copy the example and fill in your values:

```bash
cp infra/lambda-config.yaml.example infra/lambda-config-dev.yaml
# Edit infra/lambda-config-dev.yaml — set opensearch_client_config.host and
# adjust any other fields. infra/lambda-config-dev.yaml is gitignored.
```

Upload to SSM:

```bash
make push-lambda-config-dev   # reads infra/lambda-config-dev.yaml
make push-lambda-config-stg   # reads infra/lambda-config-stg.yaml
```

## 3. Env-Specific Var Files

All deployment variables — region, SSM prefix, stack tags, and infrastructure
stack references — live in a single gitignored file per environment. This keeps
company-specific values out of the repository.

Copy the example for your target environment and fill in all values:

```bash
cp infra/vars-dev.yaml.example infra/vars-dev.yaml
# Edit infra/vars-dev.yaml — set region, ssm_prefix, tags, and fill in the
# stack-name, output-key, and aws-profile for each stack output reference.
# infra/vars-dev.yaml is gitignored.
```

Stack output references follow the Sceptre `!stack_output_external` format:
`"<stack-name>::<output-key> <aws-profile>"`

| Variable                      | Used by        | Description                                               |
|-------------------------------|----------------|-----------------------------------------------------------|
| `region`                      | all stacks     | AWS region for deployment                                 |
| `ssm_prefix`                  | all stacks     | Root SSM path prefix (e.g. `/your-org/your-app/dmpworks`) |
| `tag_*`                       | all stacks     | CloudFormation stack tags                                 |
| `vpc_stack_output`            | batch-platform | VPC ID for Batch compute environments                     |
| `subnet_stack_output`         | batch-platform | Subnet ID for Batch compute environments                  |
| `security_group_stack_output` | batch-platform | Security group ID for Batch compute environments          |
| `opensearch_stack_output`     | batch-jobs     | OpenSearch domain ARN (grants container IAM access)       |
| `rds_secret_stack_output`     | batch-jobs     | Secrets Manager ARN for RDS credentials                   |

## 4. Deploy

`make deploy-dev` and `make deploy-stg` do everything in one shot: build and
push both Docker images, then run `sceptre launch` across all stacks. Sceptre
only applies changes where needed, so it is safe to run on every deploy.

```bash
make deploy-dev   # build + push images, launch all dev stacks
make deploy-stg   # build + push images, launch all stg stacks
```

To preview changes before deploying:

```bash
make diff-dev
make diff-stg
```

To push images individually without deploying:

```bash
make push-lambda-dev   # build + push Lambda image (dmpworks-lambda:latest) + update SSM digest URI
make push-batch-dev    # build + push Batch job image (dmpworks-batch:latest)
```

## 5. OpenSearch Role Setup

OpenSearch security roles must be configured after the cluster is first deployed,
and any time role ARNs change (e.g. after a Terraform/CloudFormation rename).

Connections are made via `aws-sigv4-proxy`, which signs requests locally so no
AWS auth is needed in the CLI itself.

### Prerequisites

Clone and build the proxy (one-time):

```bash
git clone git@github.com:awslabs/aws-sigv4-proxy.git
cd aws-sigv4-proxy && docker build -t aws-sigv4-proxy .
```

### Connect

Port-forward the OpenSearch domain using the CDL session tool:

```bash
session port <cluster_name>/opensearch-proxy <opensearch_endpoint> 4443:443
```

Export your AWS credentials, then run the proxy:

```bash
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_SESSION_TOKEN="..."

docker run --rm -ti \
  --network host \
  -e "AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}" \
  -e "AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}" \
  -e "AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN}" \
  aws-sigv4-proxy \
    --verbose --log-failed-requests --log-signing-process --no-verify-ssl \
    --name es --region us-west-2 \
    --host localhost:4443 \
    --sign-host us-west-2.es.amazonaws.com
```

The proxy now listens on `localhost:8080`. Pass `--client-config.port 8080` to
all `dmpworks opensearch roles` commands below.

### Commands

**Grant an AWS SSO admin principal full cluster access** (`all_access` + `security_manager`):

```bash
dmpworks opensearch roles principal --client-config.port 8080 \
  "arn:aws:iam::<account_id>:role/aws-reserved/sso.amazonaws.com/<region>/AWSReservedSSO_<profile>_<id>"
```

**Create the `aws_batch` role and map the Batch job role**:

```bash
dmpworks opensearch roles aws-batch --client-config.port 8080 \
  "arn:aws:iam::<account_id>:role/dmpworks-<env>-batch-job-role"
```

**Create the `apollo_server` role and map the Apollo ECS Task Role**:

```bash
dmpworks opensearch roles apollo-server --client-config.port 8080 \
  "arn:aws:iam::<account_id>:role/dmp-tool-<env>-ecs-apollo-EcsTaskRole-<id>"
```

Each command replaces the role definition and mapping in full, so re-running
after an ARN change or permission update is safe.

### default_role

Delete the `default_role` if it exists.
