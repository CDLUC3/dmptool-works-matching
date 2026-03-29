"""Lambda entry point for creating a ProcessWorksRunRecord."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aws_lambda_powertools.utilities.typing import LambdaContext

from dmpworks.scheduler.batch_params import generate_run_id
from dmpworks.scheduler.config import LambdaEnvSettings
from dmpworks.scheduler.dynamodb_store import create_process_works_run

logging.getLogger().setLevel(logging.INFO)
log = logging.getLogger(__name__)


def create_process_works_run_handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]:  # noqa: ARG001
    """Create a ProcessWorksRunRecord with STARTED status once all datasets are confirmed ready.

    Generates a new run_id, creates the DynamoDB record, and returns the event
    merged with the new run_id.

    Args:
        event: Workflow event containing release_date, aws_env, run_id_openalex_works,
            run_id_datacite, run_id_crossref_metadata, run_id_ror,
            run_id_data_citation_corpus, run_id_sqlmesh_prev,
            release_date_openalex_works, release_date_datacite,
            release_date_crossref_metadata, release_date_ror,
            and release_date_data_citation_corpus.
        context: Lambda context.

    Returns:
        The event dict merged with the generated run_id.
    """
    LambdaEnvSettings()

    run_id = generate_run_id()
    release_date = event["release_date"]

    log.info(f"Creating process works run: release_date={release_date} run_id={run_id}")

    create_process_works_run(
        release_date=release_date,
        run_id=run_id,
        execution_arn=event["execution_arn"],
        run_id_sqlmesh_prev=event["run_id_sqlmesh_prev"],
        run_id_openalex_works=event["run_id_openalex_works"],
        run_id_datacite=event["run_id_datacite"],
        run_id_crossref_metadata=event["run_id_crossref_metadata"],
        run_id_ror=event["run_id_ror"],
        run_id_data_citation_corpus=event["run_id_data_citation_corpus"],
        release_date_openalex_works=event["release_date_openalex_works"],
        release_date_datacite=event["release_date_datacite"],
        release_date_crossref_metadata=event["release_date_crossref_metadata"],
        release_date_ror=event["release_date_ror"],
        release_date_data_citation_corpus=event["release_date_data_citation_corpus"],
    )

    log.info(f"Created process works run: release_date={release_date} run_id={run_id}")
    return {**event, "run_id": run_id}
