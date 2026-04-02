"""Lambda entry point for detecting new dataset versions."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext

from aws_lambda_powertools.utilities.validation import validator
import boto3
import pendulum

from dmpworks.scheduler.batch_params import generate_run_id
from dmpworks.scheduler.config import VersionCheckerEnvSettings, load_lambda_config
from dmpworks.scheduler.dynamodb_store import (
    discover_latest_release,
    get_latest_known_release,
)
from dmpworks.scheduler.version_checker import (
    detect_crossref_version,
    detect_datacite_version,
    detect_dcc_version,
    detect_openalex_version,
    detect_ror_version,
)

logging.getLogger().setLevel(logging.INFO)
log = logging.getLogger(__name__)

INPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "dry_run": {"type": "boolean"},
    },
}

OUTPUT_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["triggered"],
    "properties": {
        "triggered": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["dataset", "release_date"],
                "properties": {
                    "dataset": {"type": "string"},
                    "release_date": {"type": "string"},
                },
            },
            "description": "List of datasets for which a new SFN execution was started.",
        },
        "discovered": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["dataset", "release_date"],
                "properties": {
                    "dataset": {"type": "string"},
                    "release_date": {"type": "string"},
                    "download_url": {"type": "string"},
                },
            },
            "description": "List of newly discovered releases (always populated, even in dry_run mode).",
        },
        "dry_run": {
            "type": "boolean",
            "description": "Whether this was a dry-run invocation.",
        },
    },
}


@validator(inbound_schema=INPUT_SCHEMA)
def version_checker_handler(event: dict, context: LambdaContext) -> dict:  # noqa: ARG001
    """Run version detection for all enabled datasets and start SFN executions."""
    settings = VersionCheckerEnvSettings()
    config = load_lambda_config(settings.aws_env)

    enabled = config.enabled_datasets
    log.info(f"Enabled datasets: {enabled}")

    datacite_kwargs: dict = {
        "datacite_bucket_name": config.datacite_config.bucket_name,
        "datacite_bucket_region": config.datacite_config.bucket_region,
    }
    if "datacite" in enabled:
        sm = boto3.client("secretsmanager")
        secret = sm.get_secret_value(SecretId=settings.datacite_credentials_secret_arn)
        creds = json.loads(secret["SecretString"])
        datacite_kwargs["account_id"] = creds["account_id"]
        datacite_kwargs["password"] = creds["password"]

    dataset_detectors: dict[str, tuple] = {
        "ror": (detect_ror_version, {}),
        "data-citation-corpus": (
            detect_dcc_version,
            {},
        ),
        "openalex-works": (
            detect_openalex_version,
            {
                "openalex_bucket_name": config.openalex_works_config.bucket_name,
            },
        ),
        "crossref-metadata": (
            detect_crossref_version,
            {
                "crossref_bucket_name": config.crossref_metadata_config.bucket_name,
            },
        ),
        "datacite": (detect_datacite_version, datacite_kwargs),
    }

    dry_run = event.get("dry_run", False)
    sfn = boto3.client("stepfunctions")
    triggered = []
    discovered = []

    for dataset in enabled:
        log.info(f"Processing dataset={dataset}")

        if dataset not in dataset_detectors:
            log.warning(f"Unknown dataset in enabled_datasets: {dataset}")
            continue

        detector, extra_kwargs = dataset_detectors[dataset]
        latest = get_latest_known_release(dataset=dataset)
        start_dt = pendulum.parse(latest.release_date) if latest else None

        if latest:
            log.info(f"Latest known release: dataset={dataset} release_date={latest.release_date}")
        else:
            log.info(f"No prior release found for dataset={dataset}")

        log.info(f"Checking for new release: dataset={dataset}")
        record = discover_latest_release(
            dataset=dataset,
            detector=detector,
            detector_kwargs={**extra_kwargs, "start_dt": start_dt},
        )
        if record is None:
            log.info(f"No new release for dataset={dataset}")
            continue

        discovered.append(
            {
                "dataset": dataset,
                "release_date": record.release_date,
                "download_url": record.download_url,
            }
        )

        if dry_run:
            log.info(f"Dry run — discovered but not starting SFN: dataset={dataset} release_date={record.release_date}")
            continue

        run_id = generate_run_id()
        execution_name = f"{dataset}-{record.release_date}-{run_id}"

        log.info(f"Starting SFN execution: dataset={dataset} release_date={record.release_date}")
        sfn.start_execution(
            stateMachineArn=settings.state_machine_arn,
            name=execution_name,
            input=json.dumps(
                {
                    "workflow_key": dataset,
                    "release_date": record.release_date,
                    "run_id": run_id,
                    "aws_env": settings.aws_env,
                    "bucket_name": settings.bucket_name,
                    "download_url": record.download_url,
                    "file_hash": record.file_hash,
                    "file_name": record.file_name,
                    "use_subset": config.dataset_subset.enable,
                }
            ),
        )
        log.info(f"Started SFN execution: dataset={dataset} release_date={record.release_date}")
        triggered.append(
            {
                "dataset": dataset,
                "release_date": record.release_date,
            }
        )

    return {"triggered": triggered, "discovered": discovered, "dry_run": dry_run}
