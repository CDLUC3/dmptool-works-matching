# Deploying dmpworks to AWS

This guide covers deploying all dmpworks CloudFormation stacks and operating
them after deployment. It assumes AWS credentials are already configured
(see [Submitting AWS Batch Jobs](aws-batch.md) for credential setup).

## Required Variables

Set these before running any commands in this guide:

| Variable | Description | Example |
|---|---|---|
| `ENV` | Target environment | `dev` or `stg` |
| `ECR_REGISTRY` | ECR registry hostname | `123456789012.dkr.ecr.us-west-2.amazonaws.com` |
| `SSM_PREFIX` | SSM path prefix for all parameters | `/your-org/your-app/dmpworks` |

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

## 5. Manual Trigger

Get the physical function name from CloudFormation and invoke it:

```bash
aws lambda invoke \
  --function-name $(aws cloudformation describe-stack-resource \
    --stack-name dmpworks-dev-scheduler \
    --logical-resource-id VersionCheckerFunction \
    --query "StackResourceDetail.PhysicalResourceId" \
    --output text) \
  --payload '{}' \
  --cli-binary-format raw-in-base64-out \
  response.json && cat response.json
```

The response is `{"triggered": [...]}` — one entry per dataset execution started.
