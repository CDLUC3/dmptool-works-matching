.RECIPEPREFIX := >

SHELL=/bin/bash

# Default target CPU for local development
RUST_TARGET_CPU ?= native
PYTHON_VERSION ?= 3.12

check-ecr-registry:
> @[ -n "$(ECR_REGISTRY)" ] || { echo >&2 "ECR_REGISTRY is required but not set. Aborting."; exit 1; }

check-ssm-prefix:
> @[ -n "$(SSM_PREFIX)" ] || { echo >&2 "SSM_PREFIX is required but not set. Aborting."; exit 1; }

# Ensure uv is installed
check-uv:
> @command -v uv >/dev/null 2>&1 || { echo >&2 "uv is required but not installed. Aborting."; exit 1; }

venv: check-uv
> @if [ ! -d ".venv" ]; then \
>     uv venv --seed .venv; \
> fi
> @uv tool install maturin

install: venv
> uv sync --extra dev
> RUSTFLAGS="-C target-cpu=$(RUST_TARGET_CPU)" uv tool run maturin develop --extras dev

setup-rust:
> rustup component add rustfmt clippy
> @command -v cargo-deny >/dev/null 2>&1 || cargo install cargo-deny

install-infra: venv
> uv sync --extra infra

install-release: venv
> uv sync --extra dev
> RUSTFLAGS="-C target-cpu=$(RUST_TARGET_CPU)" uv tool run maturin develop --release --extras dev --extras infra

# Build wheels for distribution/production
build-prod:
> RUSTFLAGS="-C target-cpu=$(RUST_TARGET_CPU)" uv tool run maturin build --interpreter python$(PYTHON_VERSION) --release

build-builder:
> docker build --platform linux/amd64 --provenance=false -t dmpworks-builder:latest -f Dockerfile.builder .

build-batch: build-builder
> docker build --platform linux/amd64 --provenance=false -t dmpworks-batch:latest -f Dockerfile.batch .

build-lambda: build-builder
> docker build --platform linux/amd64 --provenance=false -t dmpworks-lambda:latest -f Dockerfile.lambda .

push-lambda-dev: check-ecr-registry check-ssm-prefix build-lambda
> docker tag dmpworks-lambda:latest $(ECR_REGISTRY)/dmpworks-dev-lambda:latest
> docker push $(ECR_REGISTRY)/dmpworks-dev-lambda:latest
> aws ssm put-parameter \
    --name "$(SSM_PREFIX)/dev/LambdaEcrImageUri" \
    --value "$(ECR_REGISTRY)/dmpworks-dev-lambda@$$(aws ecr describe-images \
        --repository-name dmpworks-dev-lambda \
        --image-ids imageTag=latest \
        --query 'imageDetails[0].imageDigest' \
        --output text)" \
    --type String --overwrite

push-lambda-stg: check-ecr-registry check-ssm-prefix build-lambda
> docker tag dmpworks-lambda:latest $(ECR_REGISTRY)/dmpworks-stg-lambda:latest
> docker push $(ECR_REGISTRY)/dmpworks-stg-lambda:latest
> aws ssm put-parameter \
    --name "$(SSM_PREFIX)/stg/LambdaEcrImageUri" \
    --value "$(ECR_REGISTRY)/dmpworks-stg-lambda@$$(aws ecr describe-images \
        --repository-name dmpworks-stg-lambda \
        --image-ids imageTag=latest \
        --query 'imageDetails[0].imageDigest' \
        --output text)" \
    --type String --overwrite

fmt:
> cargo fmt --all
> .venv/bin/black ./python/dmpworks

lint:
> cargo clippy --all-features
> cargo deny check
> .venv/bin/ruff check ./python/dmpworks --fix

test-python:
> .venv/bin/pytest

test-sqlmesh:
> .venv/bin/sqlmesh -p ./python/dmpworks/sql/ test

pre-commit: venv fmt lint test test-sqlmesh

test: test-python test-sqlmesh

push-batch-dev: check-ecr-registry build-batch
> docker tag dmpworks-batch:latest $(ECR_REGISTRY)/dmpworks-dev-batch:latest
> docker push $(ECR_REGISTRY)/dmpworks-dev-batch:latest

push-batch-stg: check-ecr-registry build-batch
> docker tag dmpworks-batch:latest $(ECR_REGISTRY)/dmpworks-stg-batch:latest
> docker push $(ECR_REGISTRY)/dmpworks-stg-batch:latest

push-lambda-config-dev: check-ssm-prefix
> aws ssm put-parameter \
    --name "$(SSM_PREFIX)/dev/LambdaConfig" \
    --value "$$(cat infra/lambda-config-dev.yaml)" \
    --type String --overwrite

push-lambda-config-stg: check-ssm-prefix
> aws ssm put-parameter \
    --name "$(SSM_PREFIX)/stg/LambdaConfig" \
    --value "$$(cat infra/lambda-config-stg.yaml)" \
    --type String --overwrite

diff-dev:
> cd infra && sceptre --var-file=vars-dev.yaml diff dev

diff-stg:
> cd infra && sceptre --var-file=vars-stg.yaml diff stg

deploy-dev: push-batch-dev push-lambda-dev
> cd infra && sceptre --var-file=vars-dev.yaml launch dev

deploy-stg: push-batch-stg push-lambda-stg
> cd infra && sceptre --var-file=vars-stg.yaml launch stg


clean:
> rm -rf .venv target